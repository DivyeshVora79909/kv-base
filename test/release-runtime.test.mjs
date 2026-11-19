import test from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { writeFile, readFile, realpath, lstat } from 'node:fs/promises';
import { join, dirname, basename, relative, isAbsolute } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createRequire } from 'node:module';
import { createHash } from 'node:crypto';
import { runtimeFixture, clusterFile } from './helpers/runtime-fixture.mjs';
import { openFoundationDbStore } from '../src/foundationdb.mjs';
import { sourceKey } from '../src/keys.mjs';
import { ownerIndex, ledgerIndex } from '../examples/pipeline.mjs';
const packageRoot = process.env.KV_BASE_RELEASE_PACKAGE, skip = !clusterFile ? 'Selected real FDB cluster required' : !packageRoot ? 'Installed release package required' : false;
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
function reply(child, type, id) {
 return new Promise((resolve,reject) => {const timer=setTimeout(()=>finish(Error(`Child did not report ${type}`)),15000), onMessage=message=>{if(message?.id!==id||![type,'error'].includes(message.type))return;finish(message.type==='error'?Error(message.code):undefined,message);},onExit=code=>finish(Error(`Child exited before ${type}: ${code}`));function finish(error,value){clearTimeout(timer);child.off('message',onMessage);child.off('exit',onExit);error?reject(error):resolve(value);}child.on('message',onMessage);child.once('exit',onExit);});
}
function exited(child) { return child.exitCode !== null ? Promise.resolve(child.exitCode) : new Promise(resolve=>child.once('exit',resolve)); }

async function checkInstalledDependencies() {
 const releaseLock=await readFile(join(packageRoot,'RELEASE-LOCK.json')),lock=JSON.parse(releaseLock),manifest=JSON.parse(await readFile(join(packageRoot,'RELEASE.json'),'utf8'));
 assert.equal(digest(releaseLock),manifest.sourceIdentity.lockHash);assert.equal(lock.lockfileVersion,3);assert.deepEqual(await readFile(join(packageRoot,'package-lock.json')),releaseLock);
 const installed=await import(pathToFileURL(join(packageRoot,'src/index.mjs'))),checked=await installed.checkRelease({directory:packageRoot});assert.equal(checked.checked,true);assert.deepEqual(checked.sourceIdentity,manifest.sourceIdentity);
 const rootDirectory=await realpath(packageRoot),rootManifest=JSON.parse(await readFile(join(rootDirectory,'package.json'),'utf8'));assert.equal(rootManifest.version,lock.packages[''].version);assert.deepEqual(rootManifest.dependencies,lock.packages[''].dependencies);
 let modulesDirectory;try{const own=await lstat(join(rootDirectory,'node_modules'));assert.ok(own.isDirectory()&&!own.isSymbolicLink(),'Installed dependencies must be owned directories');modulesDirectory=join(rootDirectory,'node_modules');}catch(error){if(error.code!=='ENOENT')throw error;modulesDirectory=rootDirectory;for(const _part of rootManifest.name.split('/'))modulesDirectory=dirname(modulesDirectory);assert.equal(basename(modulesDirectory),'node_modules');}
 const found=new Set(),queue=[{directory:rootDirectory,address:'',manifest:rootManifest}];
 while(queue.length){
  const owner=queue.shift();
  for(const name of Object.keys(owner.manifest.dependencies??{})){
   let address=owner.address,expected;
   while(true){const candidate=(address?address+'/':'')+'node_modules/'+name;if(lock.packages[candidate]){expected={address:candidate,record:lock.packages[candidate]};break;}if(!address)assert.fail(`Missing locked dependency ${name}`);const parent=address.lastIndexOf('/node_modules/');address=parent<0?'':address.slice(0,parent);}
   const loader=createRequire(join(owner.directory,'package.json'));let directory=dirname(await realpath(loader.resolve(name))),dependency;
   while(true){try{const value=JSON.parse(await readFile(join(directory,'package.json'),'utf8'));if(value.name===name){dependency=value;break;}}catch(error){if(error.code!=='ENOENT')throw error;}const parent=dirname(directory);if(parent===directory)assert.fail(`Resolved dependency manifest missing ${name}`);directory=parent;}
   const within=relative(modulesDirectory,directory);assert.ok(within&&!within.startsWith('..')&&!isAbsolute(within),`Dependency ${name} must resolve inside the installed package tree`);
   assert.equal(dependency.version,expected.record.version,`${name} installed version`);assert.deepEqual(dependency.dependencies??{},expected.record.dependencies??{},`${name} declared transitive dependencies`);
   if(!found.has(expected.address)){found.add(expected.address);queue.push({directory,address:expected.address,manifest:dependency});}
  }
 }
 assert.deepEqual([...found].sort(),Object.keys(lock.packages).filter(address=>address.startsWith('node_modules/')).sort());
}

test('D5 installed release starts a real subprocess then reopens original bootstrap session and binary object state', { skip }, async t => {
 await checkInstalledDependencies();
 const f=await runtimeFixture(t);await f.runtime.close();const configurationFile=join(f.directory,'operator-configuration.json');await writeFile(configurationFile,JSON.stringify({deploymentFile:f.deploymentFile,keyringFile:f.keyringFile,logins:f.logins,contacts:f.contacts,channels:f.channels,emailUrl:f.email.baseUrl,smsUrl:f.sms.baseUrl,time:f.time,password:f.password}),{mode:0o600});
 const children=[];t.after(async()=>{for(const child of children)if(child.exitCode===null){child.kill('SIGKILL');await exited(child);}});
 const launch=async()=>{const child=fork(fileURLToPath(new URL('./fixtures/runtime-process.mjs',import.meta.url)),[],{env:{...process.env,KV_BASE_RELEASE_PACKAGE:packageRoot,KV_BASE_RUNTIME_CONFIGURATION:configurationFile},stdio:['ignore','pipe','pipe','ipc']});children.push(child);let errors='';child.stderr.on('data',chunk=>{errors+=chunk;});const ready=await reply(child,'ready',undefined).catch(error=>{throw Error(error.message+': '+errors);});assert.equal(ready.packageRoot,packageRoot);assert.equal(ready.catalogIdentity,f.catalog.identity);f.url=ready.url;return{child,ready};};
 const first=await launch();assert.equal(first.ready.bootstrap.bootstrapped,true);assert.deepEqual(await f.call('/health',undefined,undefined,'GET'),{status:200,body:{status:'ready'}});
 const begin=await f.call('/v1/proof/begin',{username:'reader',channel:'email',operationId:'release-proof'});assert.equal(begin.status,200);const drained=reply(first.child,'drained','drain-one');first.child.send({type:'drain',id:'drain-one'});await drained;
 const reader=await openFoundationDbStore({clusterFile,prefix:f.prefix});t.after(()=>reader.close());const effect=await reader.read(tx=>tx.get(sourceKey('t','identity-proof:'+begin.body.proofId))),delivered=(await f.email.ledger()).find(row=>row.value.operationId===effect.data.providerOperationId),code=/code is (\d{8})\./.exec(delivered.value.request.text)[1];
 const issued=await f.call('/v1/proof/complete',{proofId:begin.body.proofId,code,operationId:'release-complete'});assert.equal(issued.status,200);const token=issued.body.token,bytes=Buffer.from([0,255,128,13,7,0]),object=await f.call('/v1/objects/begin',{operationId:'release-object-begin',id:'release-attachment',owner:'visible',bytes:bytes.length,sha256:digest(bytes)},token);assert.equal(object.status,200);
 const uploaded=await fetch(f.url+'/v1/objects/upload',{method:'PUT',headers:{authorization:`Bearer ${token}`,'content-type':'application/octet-stream','x-upload-grant':object.body.uploadToken},body:bytes});assert.equal(uploaded.status,200);assert.equal((await f.call('/v1/objects/finalize',{operationId:'release-finalize',id:'release-attachment',uploadToken:object.body.uploadToken},token)).status,200);
 const firstExit=exited(first.child);first.child.kill('SIGTERM');assert.equal(await firstExit,0);const second=await launch();assert.deepEqual(second.ready.bootstrap,first.ready.bootstrap);
 assert.deepEqual((await f.call('/v1/query',{kind:'index',query:{model:'Entry',index:ownerIndex,partition:['ledger'],limit:10}},token)).body.records.map(record=>record.id),['visible-entry']);assert.deepEqual((await f.call('/v1/query',{kind:'summary',query:{index:ledgerIndex,partition:['ledger']}},token)).body,{summary:{balance:'20'}});
 const downloaded=await fetch(f.url+'/v1/objects/release-attachment/data',{headers:{authorization:`Bearer ${token}`}});assert.equal(downloaded.status,200);assert.deepEqual(Buffer.from(await downloaded.arrayBuffer()),bytes);for(const path of ['/v1/bootstrap','/v1/credentials','/v1/keyring','/v1/authority','/v1/raw'])assert.deepEqual(await f.call(path,{},token),{status:404,body:{error:'NOT_FOUND'}});
 const secondExit=exited(second.child);second.child.kill('SIGTERM');assert.equal(await secondExit,0);await f.open();await f.runtime.engine.check();await f.runtime.objects.checkFiles();
});
