import {openSqliteStore} from '../src/sqlite.mjs';
import {createFakeProviderServer} from '../src/providers.mjs';
const store=openSqliteStore(process.env.PROVIDER_DATABASE),server=await createFakeProviderServer({protocol:'brevo',store,secret:'private-provider-secret',afterAccepted:process.env.CRASH_AFTER_ACCEPT==='1'?()=>{process.send({phase:'accepted'});process.kill(process.pid,'SIGKILL');}:undefined});
process.send({phase:'ready',url:server.baseUrl});
process.on('message',async message=>{if(message==='close'){await server.close();await store.close();process.exit(0);}});
