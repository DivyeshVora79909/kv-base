import { decodeDomain, fail } from './values.mjs';
import { rootKey, nodeKey, occurrenceToken } from './keys.mjs';
import { compareKey, belowRange, aboveRange } from './range.mjs';

const corrupt = () => fail('CORRUPT_STATE', 'Invalid owner stream tree');

/** Internal same-snapshot inorder iterator. The caller supplies normalized cuts
 * and a budgeted fetch(keys, nodes) operation. A persistent stack avoids seeking
 * from the root for every row; loaded nodes and pending heads are never refetched.
 * Pruned subtrees remain the independent full checker's responsibility.
 */
export async function createOwnerStream(catalog, scope, range, fetch) {
  const headerKey = rootKey(catalog, scope);
  const fixedScope = structuredClone(scope), cuts = structuredClone(range);
  const domain = catalog.views[scope.view].contract.order;
  const compare = (a, b) => compareKey(domain, a, b);
  const headers = await fetch([headerKey], false);
  if (!Array.isArray(headers) || headers.length !== 1) corrupt();
  const header = headers[0];
  if (header !== undefined && (!header || typeof header.head !== 'string')) corrupt();
  const cache = new Map(), parents = new Map(), stack = [];
  let pending = header === undefined ? null : { token: header.head };
  if (pending) parents.set(pending.token, null);
  let reading = false;

  async function load(item) {
    let node = cache.get(item.token);
    if (!node) {
      const values = await fetch([nodeKey(catalog, fixedScope, item.token)], true);
      if (!Array.isArray(values) || values.length !== 1) corrupt();
      node = values[0];
      try {
        if (!node || occurrenceToken(node) !== item.token
          || !Number.isSafeInteger(node.height) || node.height < 1
          || !Number.isSafeInteger(node.size) || node.size < 1
          || ![node.left, node.right].every(link => link === null || typeof link === 'string')) corrupt();
        decodeDomain(domain, node.order);
        decodeDomain('id', node.sourceId); decodeDomain('string', node.slot);
      } catch { corrupt(); }
      // Claim links even when their subtrees will be pruned. A loaded branch
      // cannot link back to an ancestor or share a child with another branch.
      for (const token of [node.left, node.right]) {
        if (token === null) continue;
        if (parents.has(token)) corrupt();
        parents.set(token, item.token);
      }
      if (node.left === null && node.right === null && (node.height !== 1 || node.size !== 1)) corrupt();
      cache.set(item.token, node);
    }
    if ((item.lower && compare(node, item.lower) <= 0)
      || (item.upper && compare(node, item.upper) >= 0)
      || (item.parent && (node.height >= item.parent.height || node.size >= item.parent.size))) corrupt();
    return node;
  }

  async function next() {
    if (reading) fail('INVALID_READ', 'Owner stream advances must be sequential');
    reading = true;
    try {
      while (pending) {
        const item = pending, node = await load(item);
        if (belowRange(domain, node, cuts)) {
          pending = node.right === null ? null
            : { token: node.right, lower: node, upper: item.upper, parent: node };
        } else if (aboveRange(domain, node, cuts)) {
          pending = node.left === null ? null
            : { token: node.left, lower: item.lower, upper: node, parent: node };
        } else {
          stack.push({ node, upper: item.upper });
          pending = node.left === null ? null
            : { token: node.left, lower: item.lower, upper: node, parent: node };
        }
      }
      const item = stack.pop();
      if (!item) return undefined;
      pending = item.node.right === null ? null
        : { token: item.node.right, lower: item.node, upper: item.upper, parent: item.node };
      return item.node;
    } finally { reading = false; }
  }
  return { next };
}
