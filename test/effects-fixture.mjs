import { prepareCatalog } from '../src/catalog.mjs';
import { effectModels } from '../src/effects.mjs';
export const context={tenant:'tenant',actor:'alice'};
export const externalKey=Buffer.alloc(32,17);
export function definition(){return {name:'durable-effects',version:'1',models:{...effectModels(),Message:{access:{orders:{}},schema:{type:'object',properties:{status:{enum:['pending','delivered']},providerId:{type:'string'}},required:['status'],additionalProperties:false},indexes:[{version:'1',partitionFields:['status'],orderFields:['createdAt'],roles:{status:'string',createdAt:'integer'},inputs:{one:'integer'},aggregates:{count:{monoid:'sum',input:'one'}},validators:[{scope:'root',use:'message-final'}]}],memberships:[{slot:'status',index:['Message',['status'],['createdAt']],partition:[{field:'status'}],order:[{header:'createdAt'}],inputs:{one:{constant:'1'}},onMissing:'reject'}]}}};}
export function prepare(validator=()=>true){return prepareCatalog(definition(),{validators:{'message-final':{version:'1',validate:validator}}});}
export function authority(){return {tenant:'tenant',principals:[{id:'tenant',kind:'tenant',parents:[],active:true,grants:{admin:false,resources:{}}},{id:'admin',kind:'user',parents:['tenant'],active:true,grants:{admin:true,resources:{}}},{id:'alice',kind:'user',parents:['tenant'],active:true,grants:{admin:false,resources:{Credential:['create','read','update','use'],Message:['create','read','update']}}}]};}
export const message=id=>({id,model:'Message',tenant:'tenant',owner:'alice',createdAt:'1',data:{status:'pending'}});
export const email=id=>({from:'sender@example.test',to:['receiver@example.test'],subject:id,text:'locally delivered fixture'});
export const intent=(id,protocol='resend',revision='1',rate={limit:100,windowMs:60000})=>({id:'effect:'+id,provider:protocol,protocolVersion:'1',operation:'email.send',credentialId:'credential',credentialRevision:revision,request:email(id),rate});
