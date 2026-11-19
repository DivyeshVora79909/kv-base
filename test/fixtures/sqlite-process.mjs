import { openSqliteStore } from '../../src/sqlite.mjs';
const [filename,mode]=process.argv.slice(2);
let store;
const send=message=>process.send?.(message);
try{
  store=openSqliteStore(filename);
  if(mode==='attempt'){
    let called=false;
    try{await store.transact(async tx=>{called=true;await tx.set('phantom/new','bad');});send({event:'attempt',called,accepted:true});}
    catch(error){send({event:'attempt',called,code:error.code});}
  }else if(mode==='hold'){
    await store.transact(async tx=>{
      if(await tx.get('absent')!==undefined||(await tx.scan({prefix:'phantom/'})).length)throw Error('Expected absent/empty source facts');
      send({event:'ready'});await new Promise(resolve=>process.once('message',resolve));
      await tx.set('counter',(await tx.get('counter'))+1n);
    });send({event:'done'});
  }else{
    const wait=async()=>{send({event:'ready'});await new Promise(()=>{});};
    await store.transact(async tx=>{await tx.set('counter',(await tx.get('counter'))+1n);await tx.set('new',{value:9n});return {value:1n};},{receipt:{id:'original-operation',digest:'request-one'},...(mode==='before'?{beforeCommit:wait}:{afterCommit:wait})});
  }
  await store.close();process.exit(0);
}catch(error){send({event:'error',code:error.code,message:error.message});if(store)await store.close().catch(()=>{});process.exit(1);}
