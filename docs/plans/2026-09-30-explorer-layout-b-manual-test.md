# Explorer layout B: manual test

Open the explorer on a store holding an nf-core/rnaseq run (`make explore CONFIG=...`) in a desktop-width window.

1. The left column lists the pipelines. Click "nf-core/rnaseq": its runs appear with green and red dots.
2. Click the good run. The page shows its outputs A–Z with item counts, "keyed by" and file chips. Run details, Storage and Lineage check are folded at the bottom; Lineage check says "worth a look" if any published file is in no output.
3. Click "quant_salmon" (or any output). The right panel says "All N items of quant_salmon" and shows a one-line `fromStore` call. Press "Copy"; paste it somewhere and check it names the run's `lid://`.
4. Click "latest good run" in the panel. The call changes to `run: 'latest', pipeline: 'nf-core/rnaseq'`. Click "this run" to go back.
5. Open "markdup". Click a `single_end` value of `false`. The page filters to those items and shows a chip "single_end = false ✕"; the panel's call gains `where: [single_end: false]`.
6. Uncheck one row. The panel says "M of N checked" and offers "Pick these M" (the row buttons read "Pick checked (M)" and "Pick all N"). Press "Pick these M". "Picked (M)" in the top bar counts them; the panel lists them under "markdup · <run>".
7. Open the failed run's "markdup" from the left column and press "Pick all N". The panel lists two groups.
8. Type a name and press "Save as Selection". The page opens the new Selection; the panel shows its `selection:` call and the samplesheet links. ("Clear" empties the picked items instead.)
9. Narrow the window below 900 px. The left column becomes a ☰ menu and the panel a "Use · N" button (plain "Use" when it has no count); both open and close, and nothing scrolls sideways.
10. In dark mode (system setting), every page is readable.
