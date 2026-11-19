// Plain importable source facts; no store or work on import.
export const invoiceIndex = ['Invoice', ['invoice'], ['effective']];
const schema = properties => ({type:'object',additionalProperties:false,properties:Object.fromEntries(properties.map(field=>[field,{type:'string'}])),required:properties});
export const referencePathDefinition = {name:'invoice-path',version:'1',models:{
  Invoice:{schema:schema([]),indexes:[{version:'1',partitionFields:['invoice'],orderFields:['effective'],roles:{invoice:'id',effective:'integer'},inputs:{amount:'integer'},aggregates:{balance:{monoid:'sum',input:'amount'}}}]},
  Issue:{schema:schema(['invoice','effective']),references:[{slot:'invoice',field:'invoice',targets:['Invoice'],many:false,onDelete:'reject'}]},
  Tax:{schema:schema(['issue','amount']),references:[{slot:'issue',field:'issue',targets:['Issue'],many:false,onDelete:'reject'}],memberships:[{slot:'tax',index:invoiceIndex,
    partition:[{path:[{reference:'issue',target:'Issue'},{reference:'invoice',target:'Invoice'}],header:'id'}],
    order:[{path:[{reference:'issue',target:'Issue'}],field:'effective'}],inputs:{amount:{field:'amount'}},onMissing:'reject'}]},
}};
export function referencePathRecords() {
  const record=(id,model,data)=>({id,model,data,tenant:'tenant',owner:'owner',createdAt:'0'});
  return [record('A','Invoice',{}),record('B','Invoice',{}),record('issue','Issue',{invoice:'A',effective:'1'}),record('tax','Tax',{issue:'issue',amount:'20'})];
}
