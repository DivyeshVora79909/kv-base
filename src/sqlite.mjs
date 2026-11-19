import { DatabaseSync } from 'node:sqlite';
import { serialize, deserialize } from 'node:v8';
import { AsyncLocalStorage } from 'node:async_hooks';
import { isDeepStrictEqual } from 'node:util';
import { copyKvData as copy, checkKvKey, compareKvKeys as compare } from './kv-data.mjs';

const context=new AsyncLocalStorage(),format='kv-base-node-v8-1';
const error=(code,message,cause)=>Object.assign(new Error(message,cause?{cause}:undefined),{code});
const conflict=cause=>[5,6].includes(cause?.errcode&0xff);
const key=value=>Buffer.from(checkKvKey(value));
function fields(value,allowed){
  if(!value||typeof value!=='object'||![Object.prototype,null].includes(Object.getPrototypeOf(value)))throw error('INVALID_OPTIONS','Expected ordinary options');
  for(const name of Reflect.ownKeys(value)){const d=Object.getOwnPropertyDescriptor(value,name);if(typeof name!=='string'||!allowed.includes(name)||!d.enumerable||!Object.hasOwn(d,'value'))throw error('INVALID_OPTIONS','Invalid option descriptor');}
}
const encode=value=>serialize(copy(value));
function decode(value){try{if(!(value instanceof Uint8Array))throw Error('Expected bytes');return copy(deserialize(value));}catch(cause){throw error('CORRUPT_STATE','Invalid persisted value',cause);}}
function range(options={}){
  fields(options,['prefix','gte','gt','lte','lt','limit']);const result={};
  for(const name of ['prefix','gte','gt','lte','lt'])if(options[name]!==undefined)result[name]=name==='prefix'&&options[name]===''?'':checkKvKey(options[name]);
  const limit=options.limit??Infinity;if(limit!==Infinity&&(!Number.isSafeInteger(limit)||limit<0))throw error('INVALID_RANGE','Invalid scan limit');return {...result,limit};
}
function prefixEnd(bytes){const end=Buffer.from(bytes);for(let i=end.length-1;i>=0;i--)if(end[i]<255){end[i]++;return end.subarray(0,i+1);}}
function receiptSpec(value){fields(value,['id','digest']);if(Object.keys(value).length!==2)throw error('INVALID_RECEIPT','Receipt requires id and digest');return {id:checkKvKey(value.id),digest:checkKvKey(value.digest)};}
function storedReceipt(blob,id){const value=decode(blob);if(!value||Object.keys(value).length!==3||value.id!==id||typeof value.digest!=='string'||!Object.hasOwn(value,'result'))throw error('CORRUPT_STATE','Invalid receipt');return value;}

/** Persistent local KV transactions. Writable handles reserve the SQLite writer
 * before every read/write callback, including awaits. Busy conflicts are definite
 * rejection; this adapter supplies no optimistic or distributed concurrency. */
export function openSqliteStore(filename,options={}){
  fields(options,['readOnly']);if(typeof filename!=='string'||!filename.length||filename===':memory:'||!filename.isWellFormed()||options.readOnly!==undefined&&typeof options.readOnly!=='boolean')throw error('INVALID_STORAGE','Expected a persistent filename and typed options');
  const readOnly=options.readOnly??false,db=new DatabaseSync(filename,{readOnly});
  try{
    db.exec('PRAGMA busy_timeout=0');
    const version=db.prepare('PRAGMA user_version').get().user_version;
    if(version!==0&&version!==1)throw error('STORAGE_FORMAT_MISMATCH','Unsupported storage format');
    if(version===0){
      if(readOnly||db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().length)throw error('STORAGE_FORMAT_MISMATCH','Unrecognized database');
      db.exec('BEGIN IMMEDIATE');
      try{db.exec('CREATE TABLE kv(k BLOB PRIMARY KEY NOT NULL,v BLOB NOT NULL) WITHOUT ROWID;CREATE TABLE receipts(k BLOB PRIMARY KEY NOT NULL,v BLOB NOT NULL) WITHOUT ROWID;CREATE TABLE kv_meta(k TEXT PRIMARY KEY,v TEXT NOT NULL) WITHOUT ROWID;PRAGMA user_version=1');db.prepare('INSERT INTO kv_meta VALUES(?,?)').run('format',format);db.exec('COMMIT');}catch(cause){try{db.exec('ROLLBACK');}catch{}throw cause;}
    }
    if(db.prepare('SELECT v FROM kv_meta WHERE k=?').get('format')?.v!==format)throw error('STORAGE_FORMAT_MISMATCH','Unsupported value codec');
    if(!readOnly){const mode=db.prepare('PRAGMA journal_mode=WAL').get().journal_mode;if(mode!=='wal')throw error('INVALID_STORAGE','WAL is required');db.exec('PRAGMA synchronous=FULL');}
  }catch(cause){db.close();if(conflict(cause))throw error('STORAGE_CONFLICT','Database is locked',cause);throw cause;}
  const get=db.prepare('SELECT v FROM kv WHERE k=?'),put=db.prepare('INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v'),remove=db.prepare('DELETE FROM kv WHERE k=?');
  const receiptGet=db.prepare('SELECT v FROM receipts WHERE k=?'),receiptPut=db.prepare('INSERT INTO receipts(k,v) VALUES(?,?)');
  const identity=Symbol('sqlite store');let queue=Promise.resolve(),closing=false,closed=false;
  const capabilities=Object.freeze({serialized:!readOnly,volatile:false,durable:true,adapter:'node:sqlite',format,readOnly,journal:'WAL',synchronous:readOnly?'writer configuration owned by writable handle':'FULL',scans:'indexed SQLite BLOB key bounds plus touched overlay merge',guards:readOnly?'coherent SQLite read snapshot; concurrent writer exclusion is not claimed':'BEGIN IMMEDIATE excludes concurrent writable adapter transactions; point/absence/range evidence retained'});
  function run(callback,options,readonly){
    if(context.getStore()?.has(identity))return Promise.reject(error('NESTED_TRANSACTION','Same-store nested transactions are unsupported'));
    if(closing||closed)return Promise.reject(error('STORAGE_CLOSED','Store is closing or closed'));
    if(typeof callback!=='function')return Promise.reject(error('INVALID_CALLBACK','Expected a callback'));
    let fixed;
    try{fields(options,['beforeCommit','afterCommit','receipt','verifyReplay']);for(const name of ['beforeCommit','afterCommit','verifyReplay'])if(options[name]!==undefined&&typeof options[name]!=='function')throw error('INVALID_OPTIONS','Expected callback');fixed={...options,...(options.receipt===undefined?{}:{receipt:receiptSpec(options.receipt)})};}catch(cause){return Promise.reject(cause);}
    if(readOnly&&!readonly)return Promise.reject(error('READ_ONLY','Store is read-only'));
    const parents=context.getStore()??new Set();
    const task=queue.then(()=>context.run(new Set([...parents,identity]),async()=>{
      const overlay=new Map(),base=new Map(),points=new Map(),ranges=[];
      const metrics={gets:0,getMany:0,scans:0,scannedKeys:0,stagedWrites:0,sqlReads:0,sqlWrites:0};let open=true,begun=false,committed=false,commitAttempted=false;
      const assertOpen=()=>{if(!open)throw error('TRANSACTION_CLOSED','Transaction is closed');};
      function load(name){if(!base.has(name)){metrics.sqlReads++;const row=get.get(key(name));base.set(name,row?{exists:true,value:decode(row.v)}:{exists:false});}return base.get(name);}
      function guard(name){checkKvKey(name);if(!points.has(name))points.set(name,{type:'point',key:name,exists:load(name).exists});}
      const current=name=>overlay.get(name)??load(name);
      const read=name=>{guard(name);const value=current(name);return value.exists?copy(value.value):undefined;};
      function facade(forceReadonly=false){return Object.freeze({
        async get(name){assertOpen();metrics.gets++;return read(name);},
        async getMany(names){assertOpen();if(!Array.isArray(names))throw error('INVALID_KEYS','Expected keys');names.forEach(checkKvKey);metrics.getMany++;return names.map(read);},
        async set(name,value){assertOpen();if(readonly||forceReadonly)throw error('READ_ONLY','Cannot write in a read transaction');checkKvKey(name);if(value===undefined)throw error('INVALID_VALUE','Undefined denotes absence');const detached=copy(value);encode(detached);guard(name);overlay.set(name,{exists:true,value:detached});metrics.stagedWrites++;},
        async delete(name){assertOpen();if(readonly||forceReadonly)throw error('READ_ONLY','Cannot write in a read transaction');guard(name);overlay.set(name,{exists:false});metrics.stagedWrites++;},
        async scan(options={}){
          assertOpen();const r=range(options);ranges.push({type:'range',...r});metrics.scans++;
          const clauses=[],params=[];if(r.prefix){const start=Buffer.from(r.prefix),end=prefixEnd(start);clauses.push('k>=?');params.push(start);if(end){clauses.push('k<?');params.push(end);}}
          for(const [name,operator] of [['gte','>='],['gt','>'],['lte','<='],['lt','<']])if(r[name]!==undefined){clauses.push(`k${operator}?`);params.push(key(r[name]));}
          // Fetch enough committed candidates to compensate for overlay removals.
          const amount=r.limit===Infinity?Infinity:r.limit+overlay.size;
          let sql=`SELECT k,v FROM kv${clauses.length?' WHERE '+clauses.join(' AND '):''} ORDER BY k`;
          if(amount!==Infinity){sql+=' LIMIT ?';params.push(Math.min(Number.MAX_SAFE_INTEGER,amount));}
          metrics.sqlReads++;const stored=db.prepare(sql).all(...params),rows=new Map();metrics.scannedKeys+=stored.length+overlay.size;
          for(const row of stored){const name=Buffer.from(row.k).toString();rows.set(name,decode(row.v));}
          for(const [name,entry] of overlay){if(entry.exists)rows.set(name,entry.value);else rows.delete(name);}
          return [...rows].filter(([name])=>(r.prefix===undefined||name.startsWith(r.prefix))&&(r.gte===undefined||compare(name,r.gte)>=0)&&(r.gt===undefined||compare(name,r.gt)>0)&&(r.lte===undefined||compare(name,r.lte)<=0)&&(r.lt===undefined||compare(name,r.lt)<0)).sort(([a],[b])=>compare(a,b)).slice(0,r.limit).map(([name,value])=>({key:name,value:copy(value)}));
        },
        inspect(){assertOpen();return copy({metrics:{...metrics,touchedEntries:overlay.size},guards:[...points.values(),...ranges]});},
      });}
      try{
        try{db.exec(readOnly?'BEGIN':'BEGIN IMMEDIATE');begun=true;}catch(cause){if(conflict(cause))throw error('STORAGE_CONFLICT','Database is locked',cause);throw cause;}
        if(fixed.receipt){metrics.sqlReads++;const prior=receiptGet.get(key(fixed.receipt.id));if(prior){const value=storedReceipt(prior.v,fixed.receipt.id);if(value.digest!==fixed.receipt.digest)throw error('RECEIPT_MISMATCH','Operation identity has another digest');if(fixed.verifyReplay)await fixed.verifyReplay(facade(true),copy(value));open=false;db.exec('COMMIT');committed=true;return copy(value.result);}}
        const result=await callback(facade());
        const changes=[];for(const [name,entry] of overlay){const previous=load(name);if(entry.exists){if(!previous.exists||!isDeepStrictEqual(previous.value,entry.value))changes.push({type:'set',key:name,value:entry.value});}else if(previous.exists)changes.push({type:'delete',key:name});}
        const receipt=fixed.receipt?{...fixed.receipt,result:copy(result)}:undefined,receiptBytes=receipt?encode(receipt):undefined;
        open=false;
        for(const change of changes){if(change.type==='set')put.run(key(change.key),encode(change.value));else remove.run(key(change.key));metrics.sqlWrites++;}
        if(receipt){receiptPut.run(key(receipt.id),receiptBytes);metrics.sqlWrites++;}
        if(fixed.beforeCommit)await fixed.beforeCommit({changes:copy(changes)});
        commitAttempted=true;db.exec('COMMIT');committed=true;
        if(fixed.afterCommit)await fixed.afterCommit({receipt:receipt?copy(receipt):undefined});
        return result;
      }catch(cause){
        if(committed||commitAttempted&&!conflict(cause)){const failure=error('COMMIT_UNKNOWN','Commit outcome requires original receipt recovery',cause);if(fixed.receipt)failure.receiptId=fixed.receipt.id;throw failure;}
        if(conflict(cause))throw error('STORAGE_CONFLICT','Database is locked',cause);throw cause;
      }finally{open=false;if(begun&&!committed)try{db.exec('ROLLBACK');}catch{}}
    }));queue=task.catch(()=>{});return task;
  }
  async function close(){if(context.getStore()?.has(identity))throw error('NESTED_TRANSACTION','Cannot close within its transaction');if(closing){await queue;return;}closing=true;const task=queue.then(()=>{db.close();closed=true;});queue=task.catch(()=>{});return task;}
  const store=Object.freeze({capabilities,transact:(callback,options={})=>run(callback,options,false),read:callback=>run(callback,{},true),
    getReceipt:id=>run(()=>{const row=receiptGet.get(key(id));return row?storedReceipt(row.v,id):undefined;},{},true),close});return store;
}
