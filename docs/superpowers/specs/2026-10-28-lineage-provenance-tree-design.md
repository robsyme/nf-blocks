# Lineage panel: one group per upstream run

Date: 2026-10-28. Status: agreed design, not built.
Mockup: `docs/superpowers/specs/2026-10-28-lineage-provenance-tree/`
(`mockup.png` is the agreed picture; `mockup.js` draws it with the explorer's
own components; `node .../build.mjs [out-dir]` rebuilds `mockup.html` and
`mockup.png`). Changes the item page's Lineage panel from the cross-run item
lineage spec (`docs/superpowers/specs/2026-10-26-cross-run-item-lineage-design.md`
§5.5) and the matching bullets in DESIGN.md (the "Lineage panel" list near
line 6075). §5.5's rules about what may be named (no guessing, picked by
query, held nowhere, run scoping) all still hold; this spec changes how they
are laid out.

## 1. Why

The Lineage panel draws each input as its own chain (upstream item, then its
file) and stacks the chains on one vertical line (`thread()` in
`web/src/components.js`, built by `inputNodes` in `web/src/views/item.js`).
For `circle · 4` from `goofy_dubinsky`, four people items from one upstream
run, `fabulous_bartik`, read as Amina, then Dev, then Femi, then Greta, as if
each followed from the one before. The run chip repeats four times. For
`circle · 9` from `magical_ritchie` (Selection "Community", nine people
items) the panel is 33 nodes tall, and three inputs show two upstream nodes
each with nothing saying why.

What the user wants from the panel is provenance: which upstream runs this
item came from, and which items from each of those runs could have affected
its value.

## 2. Goals

- One group per upstream run, with the run named once, and that run's items
  hanging off it.
- Each input row says which upstream item it was and which file it read.
- An input whose Origins name more than one run appears under each of those
  runs, and says so on the row.
- The panel uses the explorer's visual language: Seals with kind badges
  (`sealed()`/`ref()` in `web/src/icons.js`), run chips (`runFact()`), the
  kit's tokens.

## 3. Non-goals

- No change to what lineage is recorded or how it is read. `Explorer.itemLineage`
  (`web/src/model.js`) keeps its shape; the view regroups what it returns.
- No graph layout (left-to-right DAG). The tree reads top to bottom.
- No change to the Used by panel's contents.
- No walk further upstream than one step (the upstream run's own inputs are on
  that item's page).

## 4. Anti-goals

- Naming a run the records do not name. A row sits under a run only when one
  of the input's Origins names a collection of that run. The Selection's
  `via` list is the Selection-maker's statement of where the item came from,
  and the panel respects it: an identical item published by some other run
  the Selection does not name is not shown.
- Hiding a run that an Origin does name, to make the rows add up. Rows are
  allowed to repeat across groups; the header counts inputs, not rows.

## 5. Design

The agreed picture is `mockup.png`. Two examples, both real roblab data except
two made-up runs used to show the collapsed tag:

- `circle · 4` (`#/item/bafyreidntpqvqzc6ebpiqh63byqjomrqrk7nf7afqwwxsp65rj6vc5osjm/bafyreiez7zewl7goyaaclrefrzs3rbwfycz4rmxydrpfmzijaujdihr76i`):
  one upstream run, Selection "Friends", 4 of the 9 people items.
- `circle · 9` (`#/item/bafyreih2xrnc7rm4bbj3fvrxsmdr6j35nqzlmytzpdwzfx5kg7sgqrs32q/bafyreidgx4yv434qpepw3z6qzpzihsupde25g5xwbaz3wevewhkizy4oum`):
  Selection "Community" lists all nine people from `fabulous_bartik`, and
  for Amina, Dev and Femi it also lists `sleepy_williams`' people collection,
  which holds byte-identical items. So those three inputs have two Origins
  each.

### 5.1 Page placement

The Lineage panel spans the main column. Today it shares a `grid-2` row with
Produced by (`views/item.js`, the `h('div', { class: 'grid-2' }, lineage,
producedBy)` line); that row becomes Produced by and Used by side by side, with
Lineage above it. When Used by is absent, Produced by sits alone in the row's
first column. Below 1180px `grid-2` is already one column, so narrow windows
stack as before.

### 5.2 Panel header

`Lineage`, and as meta `<R> upstream run(s) · <N> input(s)`: R counts the
distinct runs that head a group (§5.3), N counts the run's input positions for
this item (each position once, however many groups it appears in). Singular
and plural follow the counts ("1 upstream run · 4 inputs"). When R is 0 the
meta is `<N> input(s)` alone; when N is 0 there is no meta. The item's own
never-published leaves (§5.6) have no position and do not count toward N.

In the every-run view (§5.8) with two or more trees, the panel meta is
`<C> runs recorded lineage`, and each tree's wrapper carries its own
`<R> upstream run(s) · <N> input(s)` line above the tree, muted.

### 5.3 Run group

One group per upstream run, a run being the `completion_cid` of the Origin's
collection (from `runLabel(collection)`). In order:

1. Group head, on the tree's main rail with a hollow node: the run chip
   (`runFact(label, completion)`, linked to the run page in the member that
   holds it, as `originNode` does today with `hrefIn`), then muted
   `<pipeline> · <age>`. The age is what `ago(finished_at)` returns ("2d";
   the mockup's "2 days ago" is a shortcut). `runLabel` carries no pipeline,
   so the pipeline comes from one `runIdentity(completion)` call per upstream
   run, asked of the member that holds the run; when it cannot be read the
   head shows the age alone.
2. One line per (collection, Selection) pair this run's rows came through,
   indented under the head: the output as `ref('output', <output name>, #/collection/<collection>,
   { cid: collection })` (linked in the member that holds it), muted `<k> of <m> items, via`,
   then the Selection as `ref('selection', "<name>", #/selection/<cid>, { cid })`
   with its name from Claims (short CID when unnamed, as today). k counts the
   distinct upstream items in the rows under this line; m is the collection's
   item count. The view cannot reach the `collectionItemCount` SQL
   (`web/src/queries.json`, used only inside `model.js`), and
   `Explorer.collection()` is too heavy to call per line, so this needs a new
   light Explorer method, asked of the member that holds the collection. For
   a collection only in the Store Log tail the SQL answers 0 rather than
   failing; the method must then count the block's `items`, as the tail branch
   of `Explorer.collection()` does, or answer null. When m is null, or when
   the method is absent, the line says `<k> item(s), via`; it never says
   "of 0".
3. Under each such line, one row per input position, on a branch rail with a
   short tick per row.

Groups are ordered by the lowest input position they hold; rows within a line
by input position.

### 5.4 Input row

One line, four parts in a grid so the columns align within a group:

1. The upstream item: `ref('item', <title>, <item link>, { cid: item })`. The
   title is the item's own title (`itemTitle`), not `output · title`, since
   the output is named on the line above.
2. The file it supplied: `ref('file', <name>, #/content/<address>, { cid: address })`
   in mono. A file no member holds is drawn as today's dashed node says it:
   muted, with a dashed branch tick.
3. The "also" tag, when the input's Origins name other runs besides this
   group's: the word `also` in muted text outside any box, then a small boxed
   tag. With one other run the box holds that run's Seal (12px, no badge) and
   name, linked to its run page. With two or more the box reads
   `<n> other runs`, its `title` lists their names, and it is not a link.
4. The size, right aligned, muted, tabular numerals, from the input's `size`
   (`bytes()`); empty when unknown.

### 5.5 The item itself

The tree ends on the main rail with the filled accent node, as today's `here`:
the item's sealed ref with its title in bold (`<output> · <title>`), muted
`made by`, then the run chip of the run that made it (the URL's run).

### 5.6 Cases without a run group

These follow §5.5 of the cross-run spec, laid out as groups after the run
groups:

- An Origin with a null `collection` (picked by query) names no run. Its rows
  go in a group headed by the Selection (`ref('selection', ...)` and muted
  `picked by query`), with no run chip. Its item links to `#/item/-/<item>`.
- An upstream item held by no member keeps its run group when the run label
  can be read; the row's item shows its short CID and `not in any store here`,
  unlinked. When the run label cannot be read either, its rows go in a group
  headed by the collection's short CID and muted `run not in any store here`.
- Inputs with no Origin, and the never-published inputs today's panel shows
  as `· input, referenced by path`, go in a last group headed muted `Read by
  path`, file rows only (no item column), dashed when no member holds the
  bytes. An input with neither name nor address keeps today's fixed label
  (`unnamed input`). A never-published leaf whose name equals an Origin-less
  input's name is that input: one row, not two. The other leaves get a row
  each, in leaf order, after the inputs.

### 5.7 Long groups

A (collection, Selection) line with more rows than the item page's metadata
fold allows shows the first rows and folds the rest behind `+<n> more`, with
the same constants and `fold()` call as the metadata pairs (`SHOWN_PAIRS`,
`MIN_FOLDED` in `views/item.js`; fold key `lineage:<item>:<collection>:<selection>`, and `lineage:<item>:path` for Read by path).

### 5.8 Every-run view

With `-` in the URL, `itemLineage` returns every run that recorded lineage for
the item. Each such consumer run gets its own tree in its own
`[data-lineage-run]` wrapper, each ending on its own `made by` node; the
separate run chip above each thread today goes, since the `made by` node names
the run. One consumer run draws one tree with no wrapper heading, as today.

### 5.9 Runs without lineage

A run that recorded no ItemLineage, or one whose lineage cannot be drawn,
shows only the Read by path group (if any) and the item node, as today's panel
shows only the unpublished inputs and the item.

## 6. Where things live

- The tree is a new kit component next to `thread()` in `web/src/components.js`
  (its CSS in the kit: `docs/design/mockups/src/kit.css` first, then
  `web/src/kit-css.js`, per the header of `kit-css.js`), with a row in the kit
  page (`web/src/views/kit.js`) beside the Lineage thread row. Whether
  `thread()` stays for other callers is the builder's call; nothing outside
  the item page and the kit page uses it today.
- Grouping (inputs and Origins into run groups, lines and rows) is a pure
  function over `itemLineage`'s result plus the labels `readOrigin` reads, so
  it can be tested without a DOM.
- The mockup's CSS (`mockup.js`) is a starting point, not a contract. The
  contract is the picture and this text, and where they differ the text wins.
  Known differences: the age wording (§5.3), and Amina's "also 3 other runs",
  whose two made-up runs (keen_hopper, brave_lovelace) would each head a group
  of their own under §5.3, giving 4 upstream runs. The picture leaves those
  groups out to show the tag without a long page.

## 7. Verification

- Unit tests: `web/test/item-lineage-view.test.mjs` covers today's Lineage
  cases (scoping, held nowhere, picked by query, no Origin, two members,
  repeated Origins). Each keeps a test against the new layout, plus new ones
  for: grouping by run, a row under two runs with the one-run tag, the
  collapsed `n other runs` tag with its title, the `k of m` line and its
  fallback (including a tail collection), an unnamed Selection on the line
  (today's unnamed-Selection test is Used by's, not Lineage's), the header
  counts in both views, never-published leaves merged with a path input, the
  fold, and the page placement (Lineage outside the `grid-2`, Produced by and
  Used by inside it).
- Visual check against `mockup.png`, on the real page, for both example items
  at a 1280px window and once at 900px. Never point anything at
  `/tmp/cas-exploration/store` (the user's roblab store, read-only for agents);
  copy it first:

  ```
  S=<scratch dir>
  cp -R /tmp/cas-exploration/store "$S/store"
  (cd web && npm run build) && cp web/dist/index.html "$S/store/index.html"
  python3 -I gate/browser/serve.py "$S/store" <port> &
  # open http://127.0.0.1:<port>/index.html#/item/<collection>/<item>
  # with Playwright from gate/browser/node_modules, wait ~4s, screenshot
  ```

  The copied store already holds an index snapshot (`index/v7.sqlite`), so no
  indexing is needed and nothing writes to `~/.cache`. The Range server is
  required; `python -m http.server` will not do.
- The gate does not read the Lineage panel's DOM (`gate/browser_b_assert.py`
  checks the lineage blocks only), so no gate step changes and the gate does
  not check this layout; the unit tests and the visual check do.

## 8. Calls made without asking

Marked so they can be overruled while reading:

- The tag's threshold: one other run named, two or more counted.
- Group order by lowest input position; rows by input position.
- The no-run groups of §5.6 and their wording.
- The fold reuses the metadata fold's constants.
- In the every-run view the per-run chip above each tree is dropped in favour
  of each tree's `made by` node.
- The row's item shows its title only, the output being named on the line
  above.
- The every-run view's counts: `<C> runs recorded lineage` on the panel,
  per-tree counts on each wrapper (§5.2).
- Never-published leaves merge with a same-named path input and do not count
  as inputs (§5.2, §5.6).
- The group head shows the pipeline, at the cost of one `runIdentity` call per
  upstream run (§5.3).
