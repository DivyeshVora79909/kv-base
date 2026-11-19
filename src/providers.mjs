import http from 'node:http';
import { randomBytes, timingSafeEqual } from 'node:crypto';
import { assertJson, cloneData, fingerprint, fail } from './values.mjs';

const positive = value => Number.isSafeInteger(value) && value > 0;
const equal = (left, right) => { const a = Buffer.from(left), b = Buffer.from(right); return a.length === b.length && timingSafeEqual(a, b); };
function request(value, operation) {
  assertJson(value);
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail('UNSUPPORTED_EFFECT', 'Provider requires a message request');
  if (operation === 'sms.send') {
    if (typeof value.to !== 'string' || !/^\+[1-9][0-9]{7,14}$/.test(value.to) || typeof value.text !== 'string' || !value.text.length || Object.keys(value).some(key => !['to', 'text'].includes(key))) fail('UNSUPPORTED_EFFECT', 'Provider requires an E164 SMS request');
  } else if (typeof value.from !== 'string' || !Array.isArray(value.to) || !value.to.length || value.to.some(to => typeof to !== 'string') || typeof value.subject !== 'string' || typeof value.text !== 'string' || Object.keys(value).some(key => !['from', 'to', 'subject', 'text'].includes(key))) fail('UNSUPPORTED_EFFECT', 'Provider requires an email request');
  return cloneData(value);
}
const wire = {
  resend: email => ({ from: email.from, to: email.to, subject: email.subject, text: email.text }),
  brevo: email => ({ sender: { email: email.from }, to: email.to.map(address => ({ email: address })), subject: email.subject, textContent: email.text }),
};
const encode = (protocol, operation, value) => operation === 'sms.send' ? { recipient: value.to, content: value.text, type: 'transactional', sender: 'KVBase' } : wire[protocol](value);
const path = (protocol, operation) => operation === 'sms.send' ? '/v3/transactionalSMS/sms' : protocol === 'resend' ? '/emails' : '/v3/smtp/email';
const unwrap = (protocol, operation, body) => request(operation === 'sms.send' ? { to: body.recipient, text: body.content } : protocol === 'resend' ? body : { from: body.sender?.email, to: body.to?.map(value => value.email), subject: body.subject, text: body.textContent }, operation);

function createMessageProvider({ protocol, baseUrl, fetch: transport = globalThis.fetch, timeoutMs = 5000, maxRequestBytes = 1024 * 1024, lookup } = {}, operation) {
  if (!wire[protocol] || operation === 'sms.send' && protocol !== 'brevo' || typeof baseUrl !== 'string' || typeof transport !== 'function' || !positive(timeoutMs) || !positive(maxRequestBytes) || lookup !== undefined && typeof lookup !== 'function') fail('INVALID_PROVIDER', 'Invalid message provider');
  const base = new URL(baseUrl); if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password) fail('INVALID_PROVIDER', 'Invalid provider URL');
  const validate = value => { const message = request(value, operation); if (Buffer.byteLength(JSON.stringify(encode(protocol, operation, message))) > maxRequestBytes) fail('UNSUPPORTED_EFFECT', 'Provider request exceeds configured byte limit'); return message; };
  const headers = (secret, operationId, fence) => protocol === 'resend' ? { 'content-type': 'application/json', authorization: `Bearer ${secret.toString('utf8')}`, 'idempotency-key': operationId, 'x-attempt-epoch': fence } : { 'content-type': 'application/json', 'api-key': secret.toString('utf8'), 'x-operation-key': operationId, 'x-attempt-epoch': fence };
  async function send({ operationId, request: input, secret, fence = '0', incarnation }) {
    const message = validate(input); if (typeof operationId !== 'string' || !Buffer.isBuffer(secret)) fail('INVALID_PROVIDER', 'Invalid provider start');
    try {
      const response = await transport(new URL(path(protocol, operation), base), { method: 'POST', headers: {...headers(secret, operationId, fence),...(incarnation===undefined?{}:{'x-attempt-incarnation':incarnation})}, body: JSON.stringify(encode(protocol, operation, message)), signal: AbortSignal.timeout(timeoutMs) });
      if ([400, 401, 403, 404, 422, 429].includes(response.status)) return { status: 'rejected', reason: `provider HTTP ${response.status}` };
      if (!response.ok) return { status: 'unknown', reason: `provider HTTP ${response.status}` };
      let body; try { body = await response.json(); } catch { return { status: 'unknown', reason: 'provider response unreadable' }; }
      const id = protocol === 'resend' ? body.id : body.messageId;
      return typeof id === 'string' && id.length || Number.isSafeInteger(id) && id >= 0 ? { status: 'accepted', providerId: String(id) } : { status: 'unknown', reason: 'provider acceptance receipt missing' };
    } catch { return { status: 'unknown', reason: 'provider transport outcome unknown' }; }
  }
  return Object.freeze({ name: protocol, version: '1', operation, validate, send, ...(lookup ? { lookup } : {}), capabilities: Object.freeze({ protocol, idempotentSend: protocol === 'resend', authoritativeLookup: !!lookup, safeRetryAfterAbsent: protocol === 'resend', maxRequestBytes }) });
}
/** Distinct concrete HTTP email protocols. No lookup capability is inferred. */
export const createEmailProvider = options => createMessageProvider(options, 'email.send');
/** Brevo-like SMS transport with a server-selected sender. Fake conformance does
 * not certify live provider behavior or enable unknown-outcome replay. */
export const createSmsProvider = options => createMessageProvider(options, 'sms.send');

function createFakeMessageClient({ protocol, baseUrl, fetch: transport = globalThis.fetch, timeoutMs = 5000 } = {}, operation) {
  const base = new URL(baseUrl);
  async function status(kind,{operationId,secret,fence='0',incarnation,expectedIncarnations=[]}) {
    try {
      const response=await transport(new URL('/operations/'+encodeURIComponent(operationId)+'/'+kind,base),{method:'POST',headers:{'content-type':'application/json',...(protocol==='resend'?{authorization:'Bearer '+secret.toString('utf8')}:{'api-key':secret.toString('utf8')})},body:JSON.stringify({fence,...(incarnation===undefined?{}:{incarnation}),...(kind==='restore'?{expectedIncarnations}:{})}),signal:AbortSignal.timeout(timeoutMs)});
      if(!response.ok)return{status:'unknown',reason:kind==='restore'?'fake provider restore incarnation CAS rejected':'fake provider lookup unavailable'};
      const body=await response.json(),proof={fence:body.fence,...(body.incarnation===undefined?{}:{incarnation:body.incarnation}),...(kind==='restore'?{reserved:body.reserved===true}:{})};
      return typeof body.providerId==='string'?{status:'accepted',providerId:body.providerId,...proof}:body.status==='absent'?{status:'absent',...proof}:{status:'unknown',reason:'fake provider lookup malformed'};
    }catch{return{status:'unknown',reason:'fake provider lookup unknown'};}
  }
  const lookup=args=>status('fence',args),reserveRestore=args=>status('restore',args);
  const provider = createMessageProvider({ protocol, baseUrl, fetch: transport, timeoutMs, lookup }, operation);
  return Object.freeze({ ...provider,reserveRestore, capabilities: Object.freeze({ ...provider.capabilities, providerFence: true, restoreFence: true, safeRetryAfterAbsent: true }) });
}
/** Fake status API fences obsolete starts durably; do not assume it exists live. */
export const createFakeEmailClient = options => createFakeMessageClient(options, 'email.send');
export const createFakeSmsClient = options => createFakeMessageClient(options, 'sms.send');

/** Caller-owned durable provider ledger survives process death. Resend duplicates
 * coalesce; Brevo email/SMS duplicate starts deliberately deliver again. */
export async function createFakeProviderServer({ protocol, store, secret = 'test-provider-key', host = '127.0.0.1', port = 0, maxBodyBytes = 1024 * 1024, afterAccepted } = {}) {
  if (!wire[protocol] || !store?.transact || typeof secret !== 'string' || !positive(maxBodyBytes) || afterAccepted !== undefined && typeof afterAccepted !== 'function') fail('INVALID_PROVIDER', 'Invalid fake provider server');
  const stats = { starts: 0, lookups: 0, rejections: 0 }, prefix = `fake-provider/${protocol}/`;
  const readBody = async req => { const parts = []; let bytes = 0; for await (const part of req) { bytes += part.length; if (bytes > maxBodyBytes) fail('BODY_LIMIT', 'Provider body limit'); parts.push(part); } return JSON.parse(Buffer.concat(parts).toString('utf8')); };
  const server = http.createServer(async (req, res) => {
    const reply = (status, body) => { res.writeHead(status, { 'content-type': 'application/json' }); res.end(JSON.stringify(body)); };
    try {
      const authorization = protocol === 'resend' ? (req.headers.authorization?.startsWith('Bearer ') ? req.headers.authorization.slice('Bearer '.length) : undefined) : req.headers['api-key'];
      if (typeof authorization !== 'string' || !equal(authorization, secret)) { stats.rejections++; return reply(401, { error: 'credential rejected' }); }
      const url = new URL(req.url, 'http://local');
      if(req.method==='POST'&&url.pathname.startsWith('/operations/')&&(url.pathname.endsWith('/fence')||url.pathname.endsWith('/restore'))){
        stats.lookups++;const restoring=url.pathname.endsWith('/restore'),ending=restoring?'/restore':'/fence',id=decodeURIComponent(url.pathname.slice('/operations/'.length,-ending.length)),body=await readBody(req);
        if(typeof body.fence!=='string'||!/^(0|[1-9][0-9]*)$/.test(body.fence)||body.incarnation!==undefined&&(typeof body.incarnation!=='string'||!body.incarnation.length))return reply(422,{error:'invalid fence/incarnation'});
        if(restoring&&(!body.incarnation||!Array.isArray(body.expectedIncarnations)||body.expectedIncarnations.some(value=>typeof value!=='string'||!value.length)))return reply(422,{error:'invalid restore CAS'});
        const record=await store.transact(async tx=>{
          const key=prefix+id,prior=await tx.get(key);
          if(restoring){if(prior?.incarnation!==undefined&&prior.incarnation!==body.incarnation&&!body.expectedIncarnations.includes(prior.incarnation))fail('STALE_PROVIDER_INCARNATION','Restore CAS predecessor differs');}
          else if(prior?.incarnation!==undefined&&prior.incarnation!==body.incarnation)fail('STALE_PROVIDER_INCARNATION','Obsolete provider lookup');
          const maximum=BigInt(prior?.minimumFence??'0')>BigInt(body.fence)?BigInt(prior.minimumFence):BigInt(body.fence),minimumFence=String(maximum+(restoring?1n:0n));
          const next={...prior,operationId:id,minimumFence,deliveries:prior?.deliveries??0,...(body.incarnation===undefined?{}:{incarnation:body.incarnation})};await tx.set(key,next);return next;
        });
        const proof={fence:record.minimumFence,...(record.incarnation===undefined?{}:{incarnation:record.incarnation}),...(restoring?{reserved:true}:{})};
        return record.providerId?reply(200,{providerId:record.providerId,deliveries:record.deliveries,...proof}):reply(200,{status:'absent',...proof});
      }
      if (req.method === 'GET' && url.pathname.startsWith('/operations/')) {
        stats.lookups++; const id = decodeURIComponent(url.pathname.slice('/operations/'.length)), record = await store.read(tx => tx.get(prefix + id)); return record?.providerId ? reply(200, { providerId: record.providerId, deliveries: record.deliveries }) : reply(404, { status: 'absent' });
      }
      const operation = protocol === 'brevo' && url.pathname === path(protocol, 'sms.send') ? 'sms.send' : 'email.send';
      if (req.method !== 'POST' || url.pathname !== path(protocol, operation)) return reply(404, { error: 'unsupported route' });
      const operationId = protocol === 'resend' ? req.headers['idempotency-key'] : req.headers['x-operation-key'];
      if (typeof operationId !== 'string' || !operationId.length) return reply(422, { error: 'operation identity required' });
      const incarnation=req.headers['x-attempt-incarnation'];if(incarnation!==undefined&&(typeof incarnation!=='string'||!incarnation.length))return reply(422,{error:'invalid attempt incarnation'});
      const fence = req.headers['x-attempt-epoch'] ?? '0'; if (typeof fence !== 'string' || !/^(0|[1-9][0-9]*)$/.test(fence)) return reply(422, { error: 'invalid attempt epoch' });
      const message = unwrap(protocol, operation, await readBody(req)), digest = fingerprint([operation, message]); stats.starts++;
      const accepted = await store.transact(async tx => {
        const key = prefix + operationId, prior = await tx.get(key);
        if(prior?.incarnation!==undefined&&prior.incarnation!==incarnation)fail('STALE_PROVIDER_INCARNATION','Obsolete provider start incarnation');
        if (BigInt(fence) < BigInt(prior?.minimumFence ?? '0')) fail('STALE_PROVIDER_FENCE', 'Obsolete provider start');
        if (prior?.digest && prior.digest !== digest) fail('PROVIDER_OPERATION_MISMATCH', 'Provider operation payload changed');
        if (prior?.providerId && protocol === 'resend') return { ...prior, replayed: true };
        const result = { operationId, operation, digest, minimumFence:String(BigInt(prior?.minimumFence??'0')>BigInt(fence)?BigInt(prior.minimumFence):BigInt(fence)),...(incarnation===undefined?{}:{incarnation}), providerId: `${protocol}-${randomBytes(12).toString('hex')}`, deliveries: (prior?.deliveries ?? 0) + 1, request: message }; await tx.set(key, result); return result;
      });
      if (afterAccepted) await afterAccepted({ operationId, accepted, request: req, response: res });
      if (res.destroyed) return;
      return reply(201, protocol === 'resend' ? { id: accepted.providerId } : { messageId: accepted.providerId });
    } catch (error) { if (!res.destroyed) reply(error.code === 'BODY_LIMIT' ? 413 : 422, { error: error.code ?? 'malformed request' }); }
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, host, resolve); });
  const address = server.address(), baseUrl = `http://${host}:${address.port}`;
  return Object.freeze({ baseUrl, stats: () => ({ ...stats }), ledger: () => store.read(tx => tx.scan({ prefix })), close: () => new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve())) });
}
