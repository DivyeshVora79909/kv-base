# Ordered index mathematics

This page states algebra and cost contracts; current acceptance lives in
[checkpoints](checkpoints.md). The detailed
[predecessor mathematics](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/index-mathematics.md)
contains historical native examples, not local implementation status.

A source may emit zero, one or many explicitly selected occurrences. Stable slots
separate repeated participation, including two occurrences in one root. A tree is
scoped by tenant, catalog generation, view and a complete typed partition tuple.
The stable root anchor is independent of its rotating AVL head. Total order is the
declared ordered value followed by source ID and slot. Structural counts measure
occurrences, not distinct records or business quantities. Rank is computed from
subtree sizes, never globally stored:

```text
size(empty) = 0
size(node) = size(left) + 1 + size(right)
A(empty) = identity
A(node) = A(left) combine lift(node) combine A(right)
```

R1–R2 retain the size and aggregate equations above. The view becomes a structural
index signature and the ordered value becomes a typed lexicographic tuple. Reordering
components can change a prefix invariant even when the total sum stays the same.
For entries of +100 followed by −80, the minimum prefix including the empty prefix
is 0. Sorting by owner first could put −80 first, yielding a minimum of −80.
Choose each invariant's index according to the order it actually constrains.

An ordered monoid needs identity and associativity; commutativity and inverses are
unnecessary. Rotations preserve inorder and regroup operands without permuting them.
Repair descendants before ancestors. A noncommutative prefix witness must distinguish
final-total validity from every-prefix validity. For signed additive inputs, maintain
sum and minimum prefix; combine `(s1,p1)` and `(s2,p2)` as
`(s1+s2, min(p1,s1+p2))`, with an explicitly chosen empty-prefix convention.
`allPrefixes` checks a lawful witness, not an arbitrary quantified predicate.
A live empty scope checks the identity; final-member deletion cannot silently erase
an invariant whose anchor remains live.

Maintain sufficient statistics and finalize once: mean uses `(count,sum)`, not an
average of averages. Floating regrouping is approximate unless a stronger contract
is proved. Exact integers use canonical decimal JSON strings and BigInt arithmetic;
unsafe Number conversion, locale collation and implicit timestamp coercion are
excluded. Units/scales and overflow policy are part of each admitted domain.
Variable-size summaries, concatenation and arbitrary-width arithmetic have explicit
combine/storage/output costs. Schema validation cannot prove monoid laws.

Let `h = O(log(n+1))` for a valid AVL. Bounded comparisons and fixed-size lawful
summaries permit point/select/range-fold and changed-occurrence repair in `O(h)`
logical work. A known-root full fold uses `O(1)` logical reads. Contiguous raw output
of `k` occurrences uses `O(h+k)` node work; batched independent frontiers can reduce
dependent read depth to `O(h)` with adequate concurrency. Finite batch size, bytes,
cloning and buffering still cost work; a batch is not a measured network RTT.
Repeated rank-select per output would cost `O(kh)` and is not the window algorithm.

For multiple changed memberships, charge `sum(h_i)` plus actual arithmetic, references,
authority discovery, dependency closure and adapter work. Memory prefix scans are
linear in store entries; routing may visit all subscriptions. Range folding computes
a value; interval stabbing discovers affected subscribers and needs its own proof.
AVL balance alone does not establish `O(log R + matches)` stabbing. Reactive rank
windows and clocks advancing without stored input require separate semantics.

Unsorted bulk construction generally sorts in `O(n log n)`; sorted construction can
be `O(n)` plus projection/validation. Independent checking and physical cleanup may
visit all entries. No universal population counter, hidden tenant-wide writer scan,
unlimited-capacity claim or constant-size claim for variable output follows.
