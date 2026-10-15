# Explorer visual language and the block page

Date: 2026-10-14. Status: design approved in conversation, spec awaiting review.
Builds on branch `web/stable-references` (the run page's fields column, the
panel's `cas://<RunCompletion>` call, the index file with addresses).
Mockups: `.superpowers/brainstorm/22791-1792022846/content/` (`encoding.html`,
`store-layer.html`, `block-view.html`; gitignored, local only).

## 1. Purpose

The explorer names many kinds of thing: pipelines, runs, outputs, items,
files, directories, Selections, Claims, block addresses and stores. Today
they all render as plain text, a link, or a small grey box, so a reader has
to read every word to tell a run from an output from an address. The
Selection page shows the cost: each item row carries a label, a file chip,
"held in this member", key/value pills repeating the label, "via run /
output" with a Copy button, and a shortened CID, and the page uses "member"
for both a Selection's items and a store.

This design gives every kind one icon, used everywhere it is named, and one
page that renders any block by its address. The readers are both workflow
users (samples, files, runs, the `fromStore` call) and the people looking
after a store (what is held where, addresses). The workflow layer is what a
page leads with. The store layer is always present but quiet, and every
address opens the block page, which shows what content addressing means
while the reader is using it.

## 2. Icons

`web/src/icons.js` holds one inline SVG icon per kind, drawn at 14px in
`--muted`, one colour, so colour stays reserved for status (good, failed,
warning). The kinds and their icons (the mockups' shapes):

| kind | icon |
|---|---|
| pipeline | two boxes joined by a line |
| run | a play triangle |
| output | three horizontal lines |
| item | a pair of braces |
| file | a page with a folded corner |
| directory | a folder |
| Selection | a bookmark |
| name / pin (Claim) | a tag |
| address | a hash sign |
| store | a cylinder |

`icon(kind)` returns the SVG with `aria-hidden="true"`, wrapped in a span
whose `title` is the kind's name, so hovering an unfamiliar icon says what
it is. An unknown kind throws.

`ref(kind, label, href)` is an icon followed by its label, a link when
`href` is given. Views use it wherever they name a run, output, item, file,
directory, pipeline or Selection, in place of plain text or a bare link.

## 3. Addresses

`address(cid)` replaces `cid()` in `html.js` (13 uses across six files
today). It draws the address icon and the CID shortened by the existing
`shortCid` (first 10 and last 6 characters, `bafyreian3...ac6uye`), as a link to `#/block/<cid>` in
`--muted` with a light underline, its `title` the full CID, carrying
`data-address="<cid>"`. Copy is not on the address itself; the block page
has it. An address is never a page's headline: it ends a line or sits in a
row's quiet line.

There is no global "store details" switch and no per-page store fold. The
address is always there, small, and always leads to the block page.

## 4. The block page, `#/block/<cid>`

One layout for any block. The section carries `data-block="<cid>"`.

### 4.1 Header

The full CID (monospace, wrapping) with a Copy button. Under it, one line
naming the kind with its icon and linking to the friendly page where there
is one:

| block | line and link |
|---|---|
| RunCompletion | a run; "Open the run", `#/run/<cid>` |
| RunManifest | a run's start record; "Open the run" when the snapshot (`run.manifest_cid`) or the Store Log tail names its RunCompletion |
| OutputCollection | an output; `#/collection/<cid>` |
| OutputItem | an item; `#/item/-/<cid>` |
| Selection | a Selection; `#/selection/<cid>` |
| Claim | a name, pin or release; a link to its subject's block page |
| DirectoryManifest | a directory; no friendly page, its entries are the contents |
| Forget | a sweep's record of deleted blocks; no friendly page |
| raw | a file's bytes; "Who produced it", `#/content/<cid>` |

### 4.2 What the address says

The CID decoded, as a two-column list:

- format: `dag-cbor`, "structured data, which can link to other blocks", or
  `raw`, "bytes exactly as the file held them; no name, no links";
- fingerprint: the hash function (`sha2-256`) of the block's byte count,
  and the digest in hex, shortened to 8 and 8 characters with the full
  digest as its `title`;
- one fixed sentence: any copy of these bytes, anywhere, has this address;
  change one byte and it changes. For `raw` it adds that two runs writing
  the same bytes share one block.

A CID of another version, codec or hash than the store writes (DESIGN.md
§3) is shown decoded with the line "not an address this store writes".

### 4.3 Checked

`data-block-check` carries `verified`, `unchecked`, `refused` or `missing`.

- A `dag-cbor` block is fetched through `BlockFetcher.get`, which verifies
  it. The line reads "Read N bytes from <member>; their SHA-256 matches the
  address. Checked in your browser."
- A `raw` block of at most 1 MiB (`BLOCK_PAGE_RAW_CAP`, `config.js`) is
  fetched whole through `BlockFetcher.raw`, verified, and reported the same
  way.
- A larger `raw` block is not fetched whole. `BlockFetcher.partial(cid, n)`
  reads its first 4 KiB with `Range: bytes=0-4095` and takes the size from
  `Content-Range`. The line says the block was not checked, because
  checking needs all N bytes, and offers Download. Without Range support
  (a plain static server), there is no preview and the line says so.
- A hash mismatch shows "refused: these bytes do not hash to the address"
  in `.warn`, and nothing from the bytes.

`BlockFetcher.partial` keeps its own count (`partialFetches`) and never
records a block in `verified`, so Gate tier A's A4 ("every fetched block is
hash-verified") still holds for every block fetched whole.

### 4.4 Contents

- A `dag-cbor` block is drawn as DAG-JSON built from the decoded value, not
  from a string: each link `{"/": cid}` is an `address()` to its own block
  page, so a reader can walk the graph. Keys are in the block's order.
  Below it, "Raw bytes" and "DAG-JSON" downloads (`saveText` and a blob for
  the bytes).
- A block that decodes but fails the schema check is still drawn, with the
  reason above it in `.warn`.
- A `raw` block shows a text preview of its first 4 KiB when they decode as
  UTF-8 (a decoding error, or a NUL byte, means binary: "binary, N bytes"),
  and Download (the block's URL).

### 4.5 Missing blocks

A 404 or 403 reads "not in <member>; it may be held in another store",
with a link to the same `#/block/<cid>` under each other member
(`?member=<alias>`) when the page knows the member list (`members.json`).

## 5. Page changes

### 5.1 Item rows

`itemRow` (`views/common.js`) draws every list of items, on the Selection
page and the collection page.

- Line 1: `ref('item', label, item page)`, the label in bold; the row's
  action on the right (Pick, or where the item is held).
- Line 2: the item's files, each `ref('file' | 'directory', name)`; "from"
  and `ref('run', run name, run page)` / `ref('output', output, items
  view)`; then the item's `address()`, and the Item Occurrence Copy
  (`cas://<collection>/<item>`) as a small quiet button where a row has one.
- The key/value pills are removed from rows. The item page keeps them all;
  the items table keeps them as columns with click-to-filter.
- "Held in" shows only the exceptions: "held in <alias>" for another
  member, "not in any store" when no member holds it. Held here shows
  nothing. `data-held` keeps its values.

### 5.2 Selection page

- Heading: `icon('Selection')` and the name. One quiet line under it: the
  Claim icon, "named by <asserted_by> · <local time>", and the Selection's
  `address()`. A name conflict keeps its flag.
- "Members (N)" becomes "Items (N)", or "Items and Selections (N)" when it
  nests Selections. A nested Selection's row is `ref('Selection', its name,
  its page)` and its address, in place of the word "Selection" and a bare
  CID.
- The read-only notice becomes one quiet line where Rename and Delete would
  be: "Read only here: rename and delete need `nf-blocks:explore`." It
  keeps `[data-unavailable]`, and the "open this page in the writable
  member" link when that applies.

### 5.3 Other pages

Icons and addresses only; no layout change.

- Run page: `icon('run')` in the heading; `ref('output', ...)` in the
  outputs table; addresses under Run details as `address()`.
- Items table: `icon('file')` in the files column; an `address()` per row
  beside Open.
- Item page: file and directory icons on each file; `ref('run', ...)` on
  "produced by"; the address line as `address()`.
- Home, pipeline page, left column: pipeline, run and Selection icons.
- Collection page: `icon('output')` in the heading.

### 5.4 Words

- "member" means a store only. A Selection has items.
- "via" becomes "from".
- Times read in the reader's locale ("Oct 14, 4:07 PM", `whenText`), the
  ISO form in `title`.

## 6. Code structure

| file | change |
|---|---|
| `web/src/icons.js` | new: the icon set, `icon(kind)`, `ref(kind, label, href)` |
| `web/src/html.js` | `address(cid)` replaces `cid()` |
| `web/src/blocks.js` | `partial(cid, n)`, `partialFetches` |
| `web/src/model.js` | `Explorer.block(cid)`: the decoded CID, the block or its bytes, the check result, the friendly link |
| `web/src/views/block.js` | new: the block page |
| `web/src/app.js` | the `#/block/<cid>` route |
| `web/src/views/common.js` | `itemRow` as in 5.1 |
| `web/src/views/*.js` | icons and addresses as in 5.2 and 5.3 |
| `web/src/styles.js` | icon, address and block page styles, light and dark |

## 7. The page contract and the Gate

Every selector in DESIGN.md §15 keeps its element and meaning. New ones:

| selector | meaning |
|---|---|
| `[data-block]` | the block page; its value the CID |
| `[data-block-check]` | `verified`, `unchecked`, `refused` or `missing` |
| `[data-address]` | an address link; its value the CID |

Gate browser tier A gains A8: from a collection page (tier A has no
Selections), follow an item row's `[data-address]` to its block page; require `data-block-check` to be
`verified`; follow the Leaf address in its contents to the file's block
page; require that page's bytes, fetched by the Gate itself, to hash to the
CID the item block names.

DESIGN.md gains a section "Visual language and the block page" with §16's
explorer material, `#/block/<cid>` in the route list, and the three
selectors in §15's table.

## 8. Testing

Unit tests under `web/test/`, written before the code:

- `icons.js`: every kind has an icon whose span `title` names it; an
  unknown kind throws.
- `address()`: links to `#/block/<cid>`, shows the shortened text, `title`
  the full CID, carries `data-address`.
- The block page over fixture blocks: a RunCompletion, an OutputItem, an
  OutputCollection, a Selection and a Claim, each with its kind line and
  friendly link; a small raw block, verified; a large raw block, previewed
  by Range and `unchecked` (the fixture fetch gains Range); a missing
  block with links to the other members; a hash mismatch, `refused`, no
  contents; a schema failure drawn with its reason; a DAG-JSON link that is
  an `address()`.
- `BlockFetcher.partial`: reads the range, never adds to `verified`.
- Item rows: no pills; the "held in" exception; the address; "from" with
  run and output refs.
- The Selection page heading, "Items (N)", and `[data-unavailable]` on the
  read-only line.
- Existing tests whose text changes ("via" to "from") change with it.

Then `npm test`, `./gradlew check`, `make gate` (with A8), and a look at
the scratch-workflow store through the static loop
(`gate/browser/serve.py` and `?store=`).

## 9. Not in this design

- A global switch to show every address at once.
- Merging `#/content` into the block page (Gate assertion 2 counts
  `#/content`'s queries).
- "Linked from" on the block page for anything but raw blocks (which link
  to `#/content`); backlinks for structured blocks need an index query per
  kind.
- Fixing `OutputCollection.paths` ordering in the plugin (DESIGN.md, layout
  B, "Stable references").

## 10. Success

A reader can tell a run, an output, an item, a file and an address apart on
any page without reading the surrounding words; the Selection page's rows
are two lines; and from any address, one click shows what the block is,
that the browser checked it, and the blocks it links to.
