# Items table: one column per file key, files as pills

Date: 2026-10-23. Status: design agreed in conversation, spec awaiting review.
Mockups: `.superpowers/brainstorm/56917-1792764595/content/` (`pills.html` is
the agreed one; `files-column.html`, `metadata-and-files.html`,
`column-per-key.html` are earlier steps; gitignored, local only).
Changes the items page of the layout B spec
(`docs/superpowers/specs/2026-09-30-explorer-layout-b-design.md` §5.4) and
retires its file-chip rule (P9) for that page.

## 1. Why

The items page (`#/items/<run>/<output>`, `web/src/views/items.js`) ends each
row with a "files" cell: up to three file-name suffix chips and `+N`, built by
`fileChips` from the names of the item's leaves. The names are all it keeps.
Every file in a stored item sits at a field path the pipeline chose
(`markdup` has `bam`, `bai`, `metrics`, `samtools.stats`, ...; DESIGN.md: a
Leaf replaces the file inside the item's own structure), and the column throws
those paths away. Reading the nf-core/rnaseq `output-records` run, the user
took the column for a record field literally named `files` and concluded the
pipeline had not named its outputs.

What the user wants from the page: to see which named outputs each item has.
The file name matters less; the metadata columns already say which sample a
row is.

## 2. Goals

- Each top-level key of the item that holds a file gets its own column, so
  the page shows the pipeline's names for its files.
- A column makes a gap visible: an item without a file the others have shows
  an empty cell in that column.
- Every file looks the same in the table (one fixed-size pill), with its
  details on hover.
- From a pill, the user reaches the file's block page without fetching the
  file.

## 3. Non-goals

- The run page's per-output "files" column (suffixes across the output,
  `outputSuffixes`) is unchanged.
- The item page's file table (`views/item.js`, one row per leaf by name) is
  unchanged.
- The metadata columns, the "Same for every item" line, filtering by value and
  the column cap of `itemTable` are unchanged.
- No change to what is stored. Everything here is read from the item block the
  table already loads.

## 4. Anti-goals

- A click must never start a download of the whole file. Files can be many
  gigabytes (BAMs).
- The page must make no request per row, per cell or per hover beyond what it
  makes today.

## 5. Constraints

- The stored item keeps the channel item's structure; maps are DAG-CBOR, so
  their keys come back sorted and the pipeline's field order is not
  recoverable. Lists keep their order.
- Items are either records (a map at the top) or tuples (a list at the top,
  e.g. `[meta, bam, bai]`; the test fixture in `web/test/fixture.mjs` and the
  Gate's pipeline-a are tuple-shaped). A bare Leaf is also possible.
  `metadataView` already treats the first non-Leaf map in a tuple as the
  item's metadata.
- Stores may be local directories, served by `nf-blocks:explore`, or a bucket
  read straight from the browser (DESIGN.md §15 and the CORS rule near line
  1625: `range` allowed, `Content-Range`/`Content-Length` exposed). The block
  page already reads only the first 4 KiB by Range (`BlockFetcher.partial`)
  and works in all three; this design relies on that and adds nothing to it.
- `table.js` stays pure (decides shape); the views draw what it decides.

## 6. Decided

### 6.1 Which columns

A **file slot** is a Leaf anywhere in the item's value, with or without an
address. Each slot belongs to a **column key**:

- **Record item** (top-level map): the top-level key it sits under.
- **Tuple item** (top-level list): its position, written `[i]`. A position
  that holds no Leaf at any depth (the meta map, a scalar) has no column.
  (My call; the user agreed to the pill version, where names no longer
  matter.)
- **Bare Leaf item**: a single column headed `[0]`. (My call.)

The file columns are the union of the column keys over the items loaded so
far. Record keys sort alphabetically; positions sort numerically. They come
after the metadata columns and before the Open / address / Pick cell, and they
replace the single "files" column. Like the metadata columns, the set grows as
previews load and the header redraws when it changes.

No cap on the number of file columns. In the rnaseq run the widest output has
12 (`quant_merged`, one item); `bam_qc` has 6. The table already sits in a
horizontally scrolling wrapper.

A top-level key that holds both files and scalars (`bam_qc.featurecounts`
carries `id`, `meta`, `counts`, `summary`) appears in both places: its scalars
go through the metadata rules as today, its files appear as pills in its file
column.

### 6.2 What a cell shows

For one item and one column key, take the file slots under that key in the
order the item holds them (map keys sorted, list order kept):

- **No slot:** `—`, muted. The item has no such key.
- **Otherwise:** one pill per slot, in that order. When there are more than
  five, the first five show and then `+N`, a link to the item page
  (`#/item/<collection>/<item>`). (Five is my call.)

### 6.3 The pill

Every pill is the same fixed size and shape and shows an icon only, no text:

| slot | icon | link | hover (`title`) |
|---|---|---|---|
| stored file | `file` | `#/block/<address>` | field path, file name, size |
| stored directory (address codec DAG-CBOR) | `directory` | `#/block/<address>` | field path, directory name, size if known |
| no address (declined, never_published, unresolvable, unaddressed) | none; dashed outline, faded | not a link | field path, `leafReasonText(reason)`, file name if any |

- The field path is the full path inside the item, dotted, with list
  positions as `[i]`: `samtools.stats`, `rseqc.innerdistance.pdf`,
  `ballgown[3]`, `[1]` for a tuple position.
- Size is `humanBytes(leaf.size)`; omitted when the Leaf has none.
- The `file` and `directory` icons are the existing ones in `icons.js`.
- The link is a plain hash link with no `download` attribute. The block page
  shows a Range preview of a file's first bytes with Download as a separate,
  explicit action, and a directory block's entries as links.
- A pill is keyboard-focusable when it is a link, and carries an
  `aria-label` with the same text as its `title`, since it has no visible
  text.

### 6.4 Retired

`fileChips` loses its only caller (`items.js`). It is removed with its test.
DESIGN.md's P9 entry is updated to say it no longer applies to the items
page. File names no longer appear in the items table except on hover.

## 7. Left to the builder

- Where the per-item slot data comes from. `Previews` keeps `files` (names)
  and `dirs` today; it needs each slot's column key, field path, name, size,
  address and reason. Extending the preview record versus computing it in
  `table.js` from the raw value is the builder's choice, as long as
  `table.js` stays pure and nothing is fetched beyond the item block.
- Exact pill dimensions and CSS, within the explorer's visual language
  (`--muted`, the icon set, `styles.js`). Hover/focus state.
- The `data-*` attributes tests select on.
- Behaviour while an item's preview is still loading: the existing loading
  cell spans the metadata columns; extend its `colspan` to cover the file
  columns too.

## 8. Testing

- `table.js` unit tests: column union and sort across items with different
  keys (records) and positions (tuples); a tuple's meta position produces no
  column; mixed scalar/file keys; slot order; more than five slots gives
  `+N`; slots with no address; field paths with list positions.
- `items-view` / `views` DOM tests with an rnaseq-shaped record fixture
  (`markdup`-like and `bam_qc`-like items, one item missing a key, one
  declined slot) and the existing tuple fixture: header order, `—` cells,
  pill count, link targets are `#/block/<cid>`, `title`/`aria-label` text, no
  `download` attribute, no link on an address-less pill.
- The Gate browser scenario (`gate/browser_b_assert.py`) is checked for
  assertions on the old files column and updated if it has any.
