// web/src/styles.js
// The explorer's own stylesheet, on the kit's tokens (kit-css.js, injected
// first): the app shell's narrow-screen drawer and sheet, the left column's
// tree, and the classes the views drew before the kit, restyled so those views
// look like the kit until their own tasks redraw them. Tasks that add CSS
// append it in a block at the end, delimited by
// /* --- task N: <name> --- */ and /* --- end task N --- */.
export const APP_CSS = `
  /* ---------- Shell ---------- */
  /* An element with the hidden attribute stays hidden whatever display a class rule gives it (.project-form's grid was drawing Merge… open). */
  [hidden] { display: none !important; }
  #nav-toggle, #use-toggle { display: none; }
  .top .brand { text-decoration: none; }
  .top .brand:hover { text-decoration: none; }
  .brand-mark { display: inline-block; line-height: 0; flex: none; } .brand-mark svg { display: block; }
  /* Home's Health column takes the room the other columns leave and wraps inside the table. */
  .t th.health-cell { width: 32%; }
  .t td.health-cell { font-size: 12px; overflow-wrap: anywhere; }
  span.store-switch { cursor: default; color: var(--ink); }
  .store-wrap { position: relative; display: inline-flex; }
  button.store-switch { font: inherit; font-size: var(--fs-sm); color: var(--ink); }
  button.store-switch:hover { background: var(--sunken); }
  .store-menu { position: absolute; top: calc(100% + 4px); left: 0; z-index: 40; min-width: 280px; max-width: 420px; display: grid; gap: 2px; padding: 4px;
    border: 1px solid var(--line); border-radius: var(--r-md); background: var(--surface); box-shadow: 0 8px 24px rgba(0, 0, 0, .12); }
  .store-menu[hidden] { display: none; }
  .store-menu [role=option] { display: flex; flex-wrap: wrap; align-items: center; gap: 2px 8px; width: 100%; padding: 6px 8px; border: 0; border-radius: var(--r-sm);
    background: transparent; color: var(--ink); font: inherit; font-size: var(--fs-sm); text-align: left; cursor: pointer; }
  .store-menu [role=option]:hover, .store-menu [role=option].on { background: var(--sunken); }
  .store-menu [role=option][aria-selected=true] { background: var(--accent-soft); color: var(--accent-ink); font-weight: 600; }
  .store-menu .lbl { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .store-menu small { color: var(--ink-3); font-size: 11px; font-weight: 400; font-variant-numeric: tabular-nums; }
  .store-menu small.where, .store-menu small.role { flex-basis: 100%; padding-left: 16px; overflow-wrap: anywhere; }
  .store-menu small.stored { margin-left: auto; }
  .top .search input { border: 0; background: transparent; padding: 0; min-height: 0; border-radius: 0; }
  .top .search input:focus { outline: none; }
  .search input:disabled { cursor: not-allowed; }
  .search input::placeholder { color: var(--ink-3); }
  .top .picked { color: var(--ink); text-decoration: none; }
  .top .picked:hover { border-color: var(--line-2); text-decoration: none; }
  #tray-unsaved { font-size: var(--fs-xs); max-width: 22ch; line-height: 1.3; }
  .nav h2.h { font-weight: 400; }
  .nav ul { list-style: none; margin: 0; padding: 0; display: grid; gap: 2px; min-width: 0; }
  .nav .nav-toggle { display: flex; align-items: center; gap: 8px; width: 100%; padding: 5px 8px; border: 0; border-radius: var(--r-sm);
    background: none; color: var(--ink-2); font-size: var(--fs-sm); text-align: left; cursor: pointer; min-width: 0; }
  .nav .nav-toggle:hover { background: var(--sunken); }
  .nav .nav-toggle .chev { width: 10px; flex: none; color: var(--ink-3); font-size: 10px; }
  .nav .nav-toggle .lbl { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .nav .nav-toggle small { color: var(--ink-3); font-size: 11px; font-variant-numeric: tabular-nums; }
  .nav [data-nav-pipeline].here > .nav-toggle { color: var(--ink); font-weight: 600; }
  .nav .run-dot { width: 7px; height: 7px; background: var(--ink-3); }
  .nav .run-dot.ok { background: var(--ok); } .nav .run-dot.fail { background: var(--fail); }
  .nav a.sub.more-link { color: var(--ink-3); }
  .nav .warn, .nav p.muted { padding: 2px 8px; font-size: var(--fs-xs); }
  .nav #members { display: grid; gap: 2px; }
  .nav #members a small { font-size: 10.5px; }
  .nav .foot { overflow-wrap: anywhere; }
  .nav .foot code { font-family: var(--font-mono); font-size: 10.5px; }
  body:is([data-route=home], [data-route=pipeline], [data-route=latest], [data-route=run], [data-route=items],
    [data-route=collection], [data-route=item], [data-route=content]) .nav a[data-nav-link=pipelines],
  body:is([data-route=selections], [data-route=selection]) .nav a[data-nav-link=selections] {
    background: var(--accent-soft); color: var(--accent-ink); font-weight: 600; }
  .addr.flash { border-color: var(--accent); }
  .addr.flash .copy { color: var(--accent-ink); }
  .status .ico { display: inline-flex; }
  .status.neutral { color: var(--ink-3); }
  @media (max-width: 1200px) {
    .app, .app.has-rail { grid-template-columns: minmax(0, 1fr) var(--rail-w); }
    .nav { display: none; position: fixed; top: var(--top-h); bottom: 0; left: 0; width: var(--nav-w); height: auto; z-index: 40;
      box-shadow: var(--shadow-pop); }
    body[data-nav=open] .nav { display: flex; }
    #nav-toggle { display: inline-grid; }
  }
  @media (max-width: 900px) {
    .app, .app.has-rail { grid-template-columns: minmax(0, 1fr); }
    .main { padding: 16px; }
    .search { display: none; }
    .rail { display: none; position: fixed; top: auto; left: 0; right: 0; bottom: 0; height: auto; max-height: 70vh; z-index: 40;
      border-left: 0; border-top: 1px solid var(--line); box-shadow: var(--shadow-pop); }
    body[data-panel=open] .rail { display: grid; }
    #use-toggle { display: inline-flex; }
  }
  @media (max-width: 560px) { .store-wrap { display: none; } .top { gap: 8px; padding: 0 10px; } }

  /* ---------- Views drawn before the kit ---------- */
  .main h1 { font-size: var(--fs-title); font-weight: 650; letter-spacing: -.02em; overflow-wrap: anywhere; }
  .main h2, .rail h2 { font-size: var(--fs-lg); font-weight: 650; }
  .main section:not([class]) > * + *, .rail #panel-inner > * + *, .rail #panel-inner section > * + * { margin-top: 10px; }
  .main section:not([class]) > h2 { margin-top: 22px; }
  /* No specificity, so .rows, .picked-entries and later tasks' lists keep their own margins. */
  :where(.main, .rail) :where(ul, ol) { padding-left: 18px; margin: 0; }
  code, .cid { font-family: var(--font-mono); font-size: 11.5px; overflow-wrap: anywhere; }
  .muted { color: var(--ink-3); } [data-error] { color: var(--fail); } .warn, #stale[data-notice] { color: var(--releasing); }
  .ok { color: var(--ok); }
  .scroll { overflow-x: auto; min-width: 0; }
  div.scroll { background: var(--surface); border: 1px solid var(--line); border-radius: var(--r-lg); }
  pre.scroll, pre.dag-json { margin: 0; font-family: var(--font-mono); font-size: 11.5px; line-height: 1.55; background: var(--sunken);
    border: 1px solid var(--line); border-radius: var(--r-md); padding: 10px 12px; color: var(--ink-2); }
  table { border-collapse: collapse; width: 100%; font-size: var(--fs-md); }
  td, th { text-align: left; padding: 9px 10px; border-bottom: 1px solid var(--line); vertical-align: top; }
  th { color: var(--ink-3); font-weight: 500; font-size: var(--fs-xs); white-space: nowrap; vertical-align: bottom; background: var(--surface); }
  tr:last-child > td { border-bottom: 0; }
  tbody tr:hover > td, table > tr:hover > td { background: color-mix(in srgb, var(--sunken) 70%, transparent); }
  input[type=text], input[type=search], select, textarea { font: inherit; font-size: var(--fs-sm); color: var(--ink); background: var(--surface);
    border: 1px solid var(--line-2); border-radius: var(--r-md); padding: 4px 8px; min-height: 30px; }
  input[type=text]:focus, textarea:focus { outline: 2px solid var(--accent); outline-offset: 0; border-color: var(--accent); }
  form { display: grid; gap: 8px; margin: 12px 0; } fieldset { border: 1px solid var(--line); border-radius: var(--r-md); }
  /* Bare buttons only: a menu's [role=option] buttons keep their own layout (several lines, the menu's width). */
  .main button:not([class]):not([role=option]), .rail button:not([class]):not([role=option]), button.primary, button.quiet {
    display: inline-flex; align-items: center; justify-content: center; gap: 6px; height: 28px; padding: 0 10px; font-size: var(--fs-sm);
    font-weight: 600; border-radius: var(--r-md); border: 1px solid var(--line-2); background: var(--surface); color: var(--ink); cursor: pointer;
    white-space: nowrap; vertical-align: middle; }
  .main button:not([class]):not([role=option]):hover, .rail button:not([class]):not([role=option]):hover, button.quiet:hover { border-color: var(--ink-3); }
  button.primary { background: var(--accent); border-color: var(--accent); color: var(--on-accent); }
  button.primary:hover { filter: brightness(.96); }
  button.quiet { height: 22px; padding: 0 7px; font-size: var(--fs-xs); font-weight: 500; }
  .main button:disabled, .rail button:disabled { opacity: .45; cursor: not-allowed; }
  .main a.btn[aria-busy="true"] { opacity: .45; cursor: progress; }
  button.link { background: none; border: 0; padding: 0; color: var(--accent-ink); text-decoration: underline; cursor: pointer; }
  .btn-primary { background: var(--accent); border-color: var(--accent); color: var(--on-accent); }
  .btn-primary:hover { filter: brightness(.96); }
  .btn-ghost { border-color: transparent; background: none; color: var(--accent-ink); padding: 0 6px; }
  .btn-ghost:hover { border-color: transparent; background: var(--sunken); }
  .btn-icon { font-size: 15px; line-height: 1; }
  .fb-bar { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin: 6px 0 10px; } .fb-spacer { flex: 1; }
  .fb-status { font-size: var(--fs-sm); } .fb-anchor { position: relative; display: inline-block; }
  .fb-chip { display: inline-flex; align-items: center; max-width: 100%; height: 30px; border: 1px solid var(--line-2); background: var(--surface);
    border-radius: var(--r-md); font-size: var(--fs-sm); overflow: hidden; }
  .fb-chip-key { padding: 0 2px 0 10px; color: var(--ink-3); font-family: var(--font-mono); font-size: 11.5px; white-space: nowrap; overflow: hidden;
    text-overflow: ellipsis; max-width: 26em; direction: rtl; text-align: left; }
  .fb-chip-op { color: var(--ink-3); font-family: var(--font-mono); padding: 0 4px; white-space: pre; }
  .fb-chip-val { padding: 0 6px 0 0; font-weight: 600; white-space: nowrap; }
  .fb-chip-x { display: inline-flex; align-items: center; justify-content: center; align-self: stretch; width: 26px; border-left: 1px solid var(--line);
    color: var(--ink-3); text-decoration: none; font-size: 15px; }
  .fb-chip-x:hover { color: var(--ink); background: var(--sunken); text-decoration: none; }
  .fb-picker { position: absolute; z-index: 15; top: 34px; left: 0; width: 26rem; max-width: 80vw; background: var(--surface); border: 1px solid var(--line);
    border-radius: var(--r-lg); box-shadow: var(--shadow-pop); padding: 8px; }
  .fb-picker[data-open=false] { display: none; }
  .fb-input { width: 100%; height: 30px; border: 1px solid var(--line-2); border-radius: var(--r-md); padding: 0 9px; font: inherit;
    font-size: var(--fs-sm); background: var(--surface); color: var(--ink); }
  .fb-input:focus { outline: 2px solid var(--accent); outline-offset: 0; border-color: var(--accent); }
  .fb-list { max-height: 16rem; overflow: auto; margin-top: 6px; }
  .fb-item { display: flex; justify-content: space-between; gap: 12px; width: 100%; padding: 6px 8px; border: 0; border-radius: var(--r-md);
    background: none; color: var(--ink); font: inherit; font-size: var(--fs-sm); text-align: left; text-decoration: none; cursor: pointer; }
  .fb-item:hover, .fb-item:focus-visible, .fb-item.active { background: var(--sunken); outline: none; text-decoration: none; }
  .fb-path { font-family: var(--font-mono); font-size: 11.5px; word-break: break-all; }
  .fb-count { color: var(--ink-3); font-size: var(--fs-xs); white-space: nowrap; }
  .fb-head { display: flex; align-items: center; gap: 6px; padding: 0 2px 6px; } .fb-back { height: 22px; font-size: 16px; }
  .fb-type { font-family: var(--font-mono); font-size: 10.5px; color: var(--ink-3); border: 1px solid var(--line); border-radius: 3px; padding: 0 4px; }
  .fb-ops { display: inline-flex; margin: 0 0 6px; border: 1px solid var(--line); border-radius: var(--r-md); overflow: hidden; }
  .fb-op { min-width: 34px; height: 26px; border: 0; border-left: 1px solid var(--line); background: var(--surface); color: var(--ink-3);
    font-family: var(--font-mono); font-size: 11.5px; cursor: pointer; }
  .fb-op:first-child { border-left: 0; } .fb-op:hover { background: var(--sunken); }
  .fb-op[aria-pressed=true] { background: var(--ink); color: var(--surface); font-weight: 600; }
  .fb-error { color: var(--fail); font-size: var(--fs-xs); margin: 4px 2px 0; } .fb-error:empty { display: none; } .fb-note { margin: 8px 4px; font-size: var(--fs-sm); }
  .file-pill { display: inline-flex; align-items: center; justify-content: center; width: 26px; height: 18px; border: 1px solid var(--line-2);
    border-radius: 3px; margin-right: 4px; vertical-align: middle; color: var(--ink-3); background: var(--surface); text-decoration: none; }
  .file-pill:hover, .file-pill:focus-visible { border-color: var(--accent); color: var(--accent-ink); background: var(--accent-soft); outline: none; text-decoration: none; }
  .file-pill .icon { margin: 0; width: 12px; height: 12px; vertical-align: 0; pointer-events: none; }
  .file-pill:hover .icon, .file-pill:focus-visible .icon { background-color: var(--accent-ink); }
  .file-pill-empty { border-style: dashed; opacity: .6; cursor: default; font-family: var(--font-mono); font-size: 10px; }
  .file-pill-more { font-size: 11.5px; color: var(--ink-3); margin: 0 6px 0 2px; text-decoration: none; } .file-pill-more:hover { color: var(--accent-ink); }
  .items-table td, .items-table th { white-space: nowrap; vertical-align: middle; }
  .items-table th.file-key { font-family: var(--font-mono); font-size: 11.5px; font-weight: 600; color: var(--ink); }
  .items-table td.file-cell { background: color-mix(in srgb, var(--sunken) 70%, transparent); }
  .items-table .file-last, .items-table .item-actions { border-right: 1px solid var(--line); }
  button.file-pill { padding: 0; font: 600 13px/1 var(--font-ui); cursor: pointer; }
  .file-pill.picked, .file-pill.picked:hover { border-color: var(--pinned); color: var(--pinned); background: var(--pinned-bg); cursor: default; }
  .file-pill-tip { position: fixed; z-index: 20; pointer-events: none; background: var(--ink); color: var(--surface); border-radius: var(--r-md);
    padding: 6px 9px; font-size: var(--fs-xs); line-height: 1.4; max-width: 420px; box-shadow: var(--shadow-pop); }
  .file-pill-tip[data-open=false] { display: none; }
  .file-pill-tip .tip-path { font-family: var(--font-mono); font-weight: 600; } .file-pill-tip .tip-name { opacity: .85; word-break: break-all; }
  .file-pill-tip .tip-size { opacity: .7; } .file-pill-tip .tip-reason { font-family: var(--font-mono); font-size: 10.5px; opacity: .85; }
  span.ext { display: inline-block; font-family: var(--font-mono); font-size: 10.5px; padding: 0 5px; margin-right: 3px; border: 1px solid var(--line);
    border-radius: 3px; color: var(--ink-2); background: var(--surface); }
  .fileb .ext { display: inline; padding: 0; margin: 0; border: 0; background: none; color: var(--ink-3); font-size: 10px; }
  .index-link { margin-left: 6px; }
  .address { color: var(--ink-3); font-family: var(--font-mono); font-size: 11.5px; text-decoration-color: var(--line-2); white-space: nowrap; }
  .address .icon { margin-right: 2px; }
  .constant { font-size: var(--fs-xs); }
  .badge { display: inline-flex; align-items: center; font-size: var(--fs-xs); font-weight: 600; padding: 2px 8px; border-radius: 999px;
    color: var(--pinned); background: var(--pinned-bg); margin-right: 6px; white-space: nowrap; }
  details.fold { border-top: 1px solid var(--line); padding: 8px 0; } .pills + details.fold { margin-top: 6px; }
  details.fold > summary { cursor: pointer; font-weight: 600; }
  details.fold[open] > summary { margin-bottom: 8px; }
  .rows { list-style: none; padding: 0; margin: 8px 0; }
  .rows > .row { display: block; padding: 8px 0; border-bottom: 1px solid var(--line); }
  .row-head { display: flex; flex-wrap: wrap; gap: 6px; align-items: baseline; } .row-action { margin-left: auto; }
  .row-sub { margin: 2px 0 0 22px; color: var(--ink-3); font-size: var(--fs-sm); } .row-sub code { color: var(--ink); }
  .row-actions { display: flex; flex-wrap: wrap; gap: 8px; align-items: baseline; }
  /* The rail's snippet as the kit draws it (mockups 03/04/08): the code block and Copy snippet full width; the call wraps rather than scrolls. */
  .snippet { display: grid; gap: 8px; margin: 0; }
  .snippet pre.code { white-space: pre-wrap; overflow-wrap: anywhere; }
  .snippet .snippet-copy { width: 100%; justify-content: center; }
  .seg-row { gap: 6px; }
  ul.picked, .picked-entries { list-style: none; padding: 0; } .picked-entries { margin-left: 16px; font-size: var(--fs-xs); }
  .block-cid { font-size: var(--fs-lg); display: flex; gap: 6px; align-items: baseline; flex-wrap: wrap; } .block-cid code { overflow-wrap: anywhere; }
  .block-parts { display: grid; grid-template-columns: auto 1fr; gap: 4px 14px; font-size: var(--fs-sm); }
  .block-parts dt { color: var(--ink-3); } .block-parts dd { margin: 0; min-width: 0; overflow-wrap: anywhere; }
  /* ---------- #/kit: the component sheet ---------- */
  .kit-sheet { max-width: 1200px; display: grid; gap: 0; }
  .kit-intro { display: grid; gap: 6px; padding-bottom: 18px; } .kit-intro h1 { font-size: 28px; } .kit-intro p { max-width: 70ch; }
  .kit-row { display: grid; grid-template-columns: 220px minmax(0, 1fr); gap: 20px; padding: 18px 0; border-top: 1px solid var(--line); }
  .kit-lbl { display: grid; gap: 4px; align-content: start; } .kit-lbl h2 { font-size: 17px; margin-bottom: 4px; }
  .kit-lbl code { color: var(--accent-ink); } .kit-lbl p { font-size: 12.5px; color: var(--ink-3); }
  .kit-demo { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; padding: 16px; background: var(--surface); border: 1px solid var(--line);
    border-radius: var(--r-lg); min-width: 0; }
  .kit-stack { display: grid; gap: 14px; width: 100%; min-width: 0; } .kit-block { display: block; width: 100%; }
  .kit-tokens { display: grid; grid-template-columns: repeat(auto-fill, minmax(130px, 1fr)); gap: 8px; width: 100%; }
  .kit-token { border: 1px solid var(--line); border-radius: var(--r-md); overflow: hidden; }
  .kit-token > div:first-child { height: 36px; } .kit-token .mono { font-size: 11px; padding: 5px 7px; }
  @media (max-width: 760px) { .kit-row { grid-template-columns: minmax(0, 1fr); } }
  /* --- task 4: run page --- */
  .run-page { display: grid; gap: 18px; min-width: 0; }
  .run-page .page-head .title .state, .run-page .page-head .title .presence { margin-left: 2px; }
  .run-page .run-tab { display: grid; gap: 18px; min-width: 0; }
  .run-page table.outputs th button.sort { all: unset; cursor: pointer; }
  .run-page table.outputs th button.sort:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; border-radius: 2px; }
  .run-page table.outputs th[aria-sort] { color: var(--ink); }
  .run-page table.outputs th[aria-sort=descending] button.sort::after { content: " \\25BE"; }
  .run-page table.outputs th[aria-sort=ascending] button.sort::after { content: " \\25B4"; }
  .run-page table.outputs td { vertical-align: middle; }
  .run-page table.outputs tr.output-row { cursor: pointer; }
  .run-page table.outputs .kinds { align-items: center; }
  .run-page table.outputs td[data-fields] .sub, .run-page table.outputs .more { white-space: nowrap; }
  .run-page table.outputs .kinds .index { font-size: 11.5px; padding: 2px 4px; }
  .run-page table.outputs tr.group button.group-toggle { all: unset; cursor: pointer; color: var(--ink-2); }
  .run-page table.outputs tr.group button.group-toggle::before { content: "\\25B8  "; }
  .run-page table.outputs tr.group button.group-toggle[aria-expanded=true]::before { content: "\\25BE  "; }
  .run-page table.outputs tr.group button.group-toggle:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
  .run-page .lineage-counts dt.warn { color: var(--releasing); }
  .run-page details.fold { border-top: 0; padding: 0; }
  /* --- end task 4 --- */
  /* --- task 5: items grid --- */
  .items-page .page-head .items-count { font-size: 15px; }
  .items-filters { margin: 2px 0 12px; }
  .items-filters .fchip-val { font-family: var(--font-mono); font-weight: 600; }
  .items-filters .fchip .x { display: inline-grid; place-items: center; text-decoration: none; }
  .items-filters .fb-clear { font-size: 12.5px; }
  .items-body { display: grid; gap: 12px; min-width: 0; }
  .items-body .constants:empty, .items-body .more-fields:empty { display: none; }
  .items-grid td { white-space: nowrap; }
  .items-grid th, .items-grid td { padding-left: 8px; padding-right: 8px; }
  .items-grid th small { white-space: normal; }
  .items-grid td.files .kinds { flex-direction: column; align-items: flex-start; gap: 3px; }
  /* Size, Pick and Open stay at the right edge when the table scrolls; the actions cell has a fixed width so Size can sit beside it. */
  .items-grid .item-actions, .items-grid .item-size { position: sticky; z-index: 1; background: var(--surface); }
  .items-grid .item-actions { right: 0; width: 118px; min-width: 118px; box-sizing: border-box; }
  .items-grid .item-actions .btn + .btn { margin-left: 4px; }
  .items-grid .item-size { right: 118px; }
  .tbox.overflows .items-grid .item-size { box-shadow: -12px 0 10px -6px rgba(0, 0, 0, .28), -1px 0 0 var(--line-2); }
  /* A scrollbar that stays visible (macOS hides overlay ones). Chrome ignores these when scrollbar-width or -color is set. */
  .items-page .tbox::-webkit-scrollbar { height: 8px; }
  .items-page .tbox::-webkit-scrollbar-track { background: var(--sunken); }
  .items-page .tbox::-webkit-scrollbar-thumb { background: var(--line-2); border-radius: 4px; }
  .items-grid .namecell a.mono { font-weight: 500; }
  .items-grid .cell-filter { display: inline-flex; vertical-align: middle; margin-left: 6px; padding: 2px; border-radius: 3px;
    color: var(--ink-3); visibility: hidden; }
  .items-grid tr:hover .cell-filter, .items-grid .cell-filter:focus-visible { visibility: visible; }
  .items-grid .cell-filter:hover { color: var(--accent-ink); background: var(--accent-soft); }
  .items-grid .fileb-empty { border-style: dashed; opacity: .7; cursor: default; }
  .items-grid tr.group td a { color: var(--ink-3); }
  .items-grid tr.group td a:hover { color: var(--ink); }
  .more-fields { font-size: 12.5px; margin: 0; }
  .more-fields .more-field { border: 0; background: none; padding: 0; font-family: var(--font-mono); font-size: 12px; color: var(--accent-ink);
    cursor: pointer; text-decoration: underline dotted; text-underline-offset: 3px; }
  .more-fields .more-field:hover { text-decoration-style: solid; }
  /* --- end task 5 --- */
  /* --- task 6: item and file pages --- */
  .small { font-size: var(--fs-sm); }
  .pad { padding: 12px 16px; margin: 0; }
  .page-head > .row { margin-top: 10px; }
  .page-head .title h1.mono, .page-head .title h1 .mono { font-size: 19px; font-weight: 600; overflow-wrap: anywhere; }
  .page-head .subline { flex-wrap: wrap; align-items: center; }
  .preview-sheet:empty { display: none; }
  .preview-box { max-height: 520px; overflow: auto; }
  .preview-box .t th { z-index: 1; }
  .preview-lines { margin: 0; padding: 10px 0; max-height: 520px; overflow: auto; counter-reset: ln; font-family: var(--font-mono); font-size: 12px; line-height: 1.55; background: var(--surface); }
  .preview-lines code { display: block; padding: 0 16px 0 0; white-space: pre; counter-increment: ln; font: inherit; }
  .preview-lines code::before { content: counter(ln); display: inline-block; width: 4ch; margin-right: 12px; padding-right: 8px; text-align: right; color: var(--ink-3); border-right: 1px solid var(--line); box-sizing: content-box; }
  .preview-frame { display: block; width: 100%; height: 560px; border: 0; background: #fff; }
  .panel input#pin-note { flex: 1; min-width: 220px; height: 32px; padding: 0 10px; border: 1px solid var(--line-2); border-radius: var(--r-md); background: var(--surface); color: var(--ink); font: inherit; }
  .panel .pins { list-style: none; margin: 8px 0; padding: 0; display: grid; gap: 6px; }
  .runname { display: inline-flex; align-items: center; gap: 6px; }
  /* --- end task 6 --- */
  /* --- task 7: selection, picked, compose --- */
  .compose-body { display: grid; gap: 16px; min-width: 0; }
  .picked-list { display: grid; }
  .pentry { display: flex; align-items: center; gap: 8px; min-width: 0; padding: 7px 0; border-bottom: 1px solid var(--line); }
  .picked-list > .pentry:last-child { border-bottom: 0; }
  .pentry .pname { font-weight: 600; min-width: 0; overflow-wrap: anywhere; }
  .pentry code.cid { color: var(--ink-3); font-size: var(--fs-xs); }
  .pentry .btn { margin-left: auto; }
  ul.picked .pentry { padding: 3px 0; border-bottom: 0; } ul.picked .pentry .btn { display: none; }
  ul.picked .pentry:hover .btn { display: inline-flex; }
  .t td.files .kinds { flex-wrap: nowrap; }
  .compose-actions { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
  .compose-actions input { min-width: 220px; }
  .page-head .title input#rename-name { height: 32px; width: 150px; }
  .page-head .title .claim { font-weight: 400; }
  #pin-area .panel .pb { display: grid; gap: 8px; }
  /* The Selection's items fit beside the rail at 1440 as the items grid does: tighter cells, file chips stacked, From wrapping after its dot. */
  .sel-grid th, .sel-grid td { padding-left: 8px; padding-right: 8px; }
  .sel-grid td.files .kinds { flex-direction: column; align-items: flex-start; gap: 3px; }
  .sel-grid td.from-cell { min-width: 120px; }
  .sel-grid td.from-cell .from + .from { margin-top: 4px; }
  .sel-grid td.from-cell .from, .sel-grid td.from-cell .from > span { display: flex; align-items: center; flex-wrap: wrap; gap: 4px 6px; }
  .sel-grid td.from-cell .from > span { display: inline-flex; }
  /* --- end task 7 --- */
  /* --- p1a task 2: samples tab --- */
  .samples-tab, .samples-body { display: grid; gap: 12px; min-width: 0; }
  .samples-filters .samples-chips { display: contents; }
  .samples-filters .samples-match { font-size: 12.5px; }
  .samples-filters .btn[hidden] { display: none; }
  .samples-filters .fchip.bad input { border-color: var(--fail); color: var(--fail); }
  .samples-grid td { vertical-align: middle; }
  /* Fits beside the rail at 1440: tighter cells, and the has-chips wrap rather than push Size out of view. */
  .samples-grid th, .samples-grid td { padding-left: 8px; padding-right: 8px; }
  .samples-grid td[data-outputs] { min-width: 96px; }
  .samples-more { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; font-size: 12.5px; margin: 0; }
  .samples-more[hidden] { display: none; }
  .samples-unread { font-size: 12.5px; margin: 0; }
  .fb-group + .fb-group { margin-top: 6px; }
  .fb-group-name { padding: 4px 8px 2px; font-family: var(--font-mono); font-size: 10.5px; color: var(--ink-3); }
  .fb-item[aria-pressed=true] { font-weight: 600; }
  /* --- end p1a task 2 --- */
  /* --- p1a task 5: items grid --- */
  /* The funnel has its own slot beside the text in every cell (hidden until hover, but it keeps its width), so it never covers a value. */
  .items-grid .namecell { gap: 6px; }
  .items-grid .namecell .cell-filter { margin: 0 0 0 2px; flex: none; }
  .items-grid td.value.r .cell-filter { margin-left: 4px; }
  .items-grid .filemenu > summary { list-style: none; cursor: pointer; }
  .items-grid .filemenu > summary::-webkit-details-marker { display: none; }
  .items-grid .filemenu[open] > summary { border-color: var(--accent); }
  /* Fixed to the viewport, so the table box's sideways scroll cannot clip it; views/items.js places it from the chip, and it stays hidden until then. */
  .items-grid .filemenu:not([data-placed]) .filemenu-list { visibility: hidden; }
  .items-grid .filemenu-list { position: fixed; z-index: 20; display: grid; min-width: 220px; max-width: 420px;
    max-height: 260px; overflow-y: auto; padding: 4px; background: var(--surface); border: 1px solid var(--line-2); border-radius: var(--r-sm);
    box-shadow: 0 6px 18px rgba(0, 0, 0, .18); }
  .items-grid .filemenu-list a { display: flex; justify-content: space-between; gap: 14px; padding: 3px 6px; border-radius: 3px; font-size: 12px; color: var(--ink-2); text-decoration: none; }
  .items-grid .filemenu-list a:hover { background: var(--accent-soft); color: var(--accent-ink); }
  .items-grid .filemenu-list .name { overflow: hidden; text-overflow: ellipsis; }
  .items-grid .filemenu-list .size { color: var(--ink-3); font-family: var(--font-mono); font-size: 11px; }
  .items-grid .filemenu-list .filemenu-item { margin-top: 3px; border-top: 1px solid var(--line); border-radius: 0; color: var(--ink-3); }
  /* --- end p1a task 5 --- */
  /* --- p1a task 4: search --- */
  .top .search { position: relative; }
  .top .search:focus-within { border-color: var(--line-2); }
  .search-menu { position: absolute; top: calc(100% + 4px); left: 0; right: 0; z-index: 40; display: block; max-height: 70vh; overflow: auto;
    padding: 4px 0; background: var(--surface); border: 1px solid var(--line); border-radius: var(--r-md); box-shadow: var(--shadow-pop); color: var(--ink); }
  .search-menu[hidden] { display: none; }
  .search-menu .grp { display: block; padding: 4px 0; }
  .search-menu .grp + .grp, .search-menu .none + .grp { border-top: 1px solid var(--line); }
  .search-menu .eyebrow { display: block; padding: 4px 12px 2px; }
  .search-menu a { display: flex; align-items: baseline; gap: 10px; min-width: 0; padding: 5px 12px; color: var(--ink); text-decoration: none; }
  .search-menu a .lbl { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: 500; }
  .search-menu a .sub { margin-left: auto; flex: none; white-space: nowrap; }
  .search-menu a.on, .search-menu a:hover { background: var(--sunken); text-decoration: none; }
  .search-menu .none { display: block; padding: 6px 12px; color: var(--ink-3); }
  .search-page { display: grid; gap: 18px; min-width: 0; }
  .search-scope, .search-tip { font-size: 12.5px; }
  .search-page .t td code.cid { color: var(--ink-3); font-size: var(--fs-xs); }
  /* --- end p1a task 4 --- */
  /* --- p1a task 3: hand-off --- */
  .handoff { display: grid; gap: 16px; }
  .rail #panel-inner .handoff section > * + * { margin-top: 0; }
  .handoff .opt + .eyebrow { margin-top: 6px; }
  .handoff .opt .what { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .handoff .opt .note2 a { white-space: nowrap; }
  .handoff-total { justify-content: space-between; font-size: 12.5px; }
  .handoff-save { flex-wrap: nowrap; }
  .handoff-name { flex: 1; min-width: 0; height: 32px; padding: 0 10px; border: 1px solid var(--line-2); border-radius: var(--r-md);
    background: var(--surface); color: var(--ink); font: inherit; font-family: var(--font-mono); font-size: 12.5px; }
  .preview-box .t th.hl { background: var(--accent-soft); color: var(--accent-ink); }
  .preview-box .t td.hl { background: var(--accent-soft); }
  /* --- end p1a task 3 --- */
  /* P1b identity */
  .top a.me { text-decoration: none; white-space: nowrap; min-width: 0; }
  .top a.me .who { overflow: hidden; text-overflow: ellipsis; max-width: 260px; }
  .top a.me:hover .who { text-decoration: underline; }
  .top a.me.warn { white-space: nowrap; height: 28px; padding: 0 10px 0 4px; border-radius: var(--r-md); background: var(--releasing-bg); color: var(--releasing); }
  .top a.me.warn .who { max-width: none; }
  .claim.old { color: var(--ink-3); }
  [data-rail-claims] p.muted { font-size: 12px; }
  a.av.anon { text-decoration: none; cursor: pointer; }
  .identity-page .signing { display: flex; align-items: center; gap: 12px; }
  .identity-page .signing .av { width: 40px; height: 40px; font-size: 14px; }
  .identity-page .principal { font-weight: 600; font-size: var(--fs-lg, 16px); }
  .identity-page ol { margin: 8px 0 0; padding-left: 20px; display: grid; gap: 6px; }
  /* --- end P1b identity --- */
  /* P2 storage view */
  body[data-route=storage] .nav a[data-nav-link=storage] { background: var(--accent-soft); color: var(--accent-ink); font-weight: 600; }
  .nav .nav-ico-wrap { display: inline-flex; line-height: 0; flex: none; }
  .storage { display: grid; gap: 16px; min-width: 0; }
  .storage .storage-asof { font-size: 13px; }
  .storage .storage-lead { margin: 0; font-size: 13px; max-width: 80ch; }
  .storage-summary { max-width: 560px; }
  .storage-summary .pb { gap: 10px; }
  .storage .lock { display: inline-flex; align-items: center; gap: 4px; font-size: 11px; }
  .storage .lock-ico { display: inline-flex; line-height: 0; }
  .storage .storage-detail { display: flex; gap: 8px; align-items: center; font-size: 17px; margin: 6px 0 0; }
  .storage .t tr.group td { background: var(--sunken); font-weight: 600; }
  .storage .t tr.group .namecell a { font-weight: 600; }
  .storage .storage-foot { margin: 0; padding: 10px 16px 12px; font-size: 12px; }
  .storage .storage-stale { margin: 0; font-size: 13px; }
  .storage .storage-bar-asof { margin: 0; font-size: 12px; }
  @media (max-width: 1100px) { .storage .storage-grid { grid-template-columns: minmax(0, 1fr); } }
  /* P3 multi-store card row (mockup 09): one card per store, the one broken down below outlined in ink */
  .storage .storage-card { display: block; color: inherit; text-decoration: none; min-width: 0; }
  .storage .storage-card .pb { display: grid; gap: 10px; }
.storage .storage-card-main { display: grid; gap: 10px; color: inherit; text-decoration: none; min-width: 0; }
  .storage .storage-card-body { display: grid; gap: 10px; min-width: 0; }
  .storage a.storage-card-main:hover b { text-decoration: underline; }
  .storage .storage-card-actions { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; }
  .storage .storage-card-actions:empty { display: none; }
  .storage .storage-card-actions [data-index-why] { flex-basis: 100%; margin: 0; font-size: 12px; }
  .storage .storage-card-body [data-index-error] { color: var(--fail); }
  .storage .storage-card.on { border-color: var(--ink); box-shadow: inset 0 0 0 1px var(--ink); }
  .storage .storage-caption { margin: -6px 0 0; font-size: 12px; }
  /* A route without the rail (panel.js showsRail: Storage, Copy and the project pages, as mockups 09, 11 and 13 draw them) gives its width to the page. */
  .app:not(.has-rail) .rail, .app:not(.has-rail) #use-toggle { display: none; }
  @media (max-width: 1200px) { .app:not(.has-rail) { grid-template-columns: minmax(0, 1fr); } }
  /* end P2 storage view */
  /* P2 decision points: the run's Storage tab, Release's inline confirmation, what Pin and Unpin keep */
  .release-confirm { padding: 18px; display: grid; gap: 14px; }
  .release-confirm[hidden] { display: none; }
  .release-confirm h2 { font-size: 17px; margin: 0; }
  .release-confirm .sub { font-size: 12px; }
  .release-confirm .nowrap { white-space: nowrap; }
  .release-confirm .grid-3 { grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); }
  .release-confirm .kept { font-size: 13.5px; font-weight: 600; margin-top: 6px; }
  .release-confirm .release-unknown { font-size: 13.5px; margin-top: 6px; }
  .release-confirm .note { margin: 0; }
  .run-page [data-run-storage] .pb { display: grid; gap: 12px; }
  .run-page [data-release-compare] td a { color: inherit; }
  .retention [data-pinned-by] { margin: 6px 0 0; }
  /* end P2 decision points */
  /* P3 presence: a store still being asked is a faint ring until its HEAD answers */
  .presence .dot.pending { opacity: .35; }
  td .presence + .state { margin-left: 6px; }
  /* --- task P3-4: copy --- */
  .copy-page .depths { display: grid; gap: 8px; }
  .copy-page .depth-card { display: grid; grid-template-columns: auto minmax(0, 1fr) auto; gap: 10px; padding: 12px; cursor: pointer; }
  .copy-page .depth-card.on { border-color: var(--accent-ink); box-shadow: inset 0 0 0 1px var(--accent-ink); }
  .copy-page .depth-text { display: grid; gap: 2px; }
  .copy-page .small { font-size: 12px; }
  .copy-page .depth-card .figures { font-size: 12px; text-align: right; }
  .copy-page .depth-card .figures > span { display: block; }
  .copy-page .depth-card .depth-transfer { margin-top: 4px; font-size: 11px; }
  .copy-page a.addr { text-decoration: none; }
  .copy-page a.addr:hover { border-color: var(--accent); }
  .copy-page .chain { padding: 12px; background: var(--sunken); border-radius: var(--r-md); gap: 6px; margin-top: 8px; }
  .copy-page .fact.dashed { opacity: .55; border-style: dashed; }
  .copy-page .plan-cells { grid-template-columns: repeat(4, minmax(0, 1fr)); }
  @media (max-width: 1180px) { .copy-page .plan-cells { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
  .copy-page .plan-notes { display: grid; gap: 8px; margin-top: 12px; }
  .copy-page .plan-notes p, .copy-page .release-offer p { margin: 4px 0; }
  .copy-page .findings, .copy-page .failures { margin: 4px 0; padding-left: 18px; }
  .copy-page .claims-carry { display: grid; gap: 8px; }
  .copy-page .carry { display: inline-flex; flex-wrap: wrap; gap: 6px; align-items: center; }
  .copy-page pre.code { white-space: pre-wrap; overflow-wrap: anywhere; }
  .copy-page pre.code.stale { opacity: .55; }
  .copy-page [data-copy-result] .pb { display: grid; gap: 8px; }
  .copy-page [hidden] { display: none; }
  .store-menu [role=option]:disabled { color: var(--ink-3); cursor: not-allowed; background: transparent; }
  /* --- end task P3-4 --- */
  /* P4 Task 2: the Projects list, New project and the suggestions. */
  .projects-page { display: grid; gap: 16px; min-width: 0; }
  .projects-page [hidden] { display: none; }
  .projects-page .project-form { max-width: 560px; margin: 0; padding: 14px 16px; border: 1px solid var(--line); border-radius: var(--r-lg); background: var(--surface); }
  .projects-page .project-form label { display: grid; gap: 4px; }
  .projects-page .project-form p { margin: 0; }
  .projects-page .project-desc { font-size: var(--fs-xs); margin-top: 2px; max-width: 60ch; }
  .projects-page .store-figure { white-space: nowrap; font-variant-numeric: tabular-nums; }
  .projects-page .project-people { gap: 4px; }
  .projects-page .suggestion { gap: 8px; flex-wrap: wrap; }
  /* P4 Task 3: one project's page. */
  .project-page { display: grid; gap: 16px; min-width: 0; }
  .project-page [hidden] { display: none; }
  .project-page .project-people { gap: 4px; }
  .project-page ul.plain { list-style: none; margin: 0; padding: 0; display: grid; gap: 8px; }
  .project-page .project-fields { display: grid; gap: 14px; }
  .project-page .project-fields > div { display: grid; gap: 6px; justify-items: start; }
  .project-page .project-fields > div.row { display: flex; }
  .project-page .project-fields p { margin: 0; }
  .project-page .project-fields .row { gap: 8px; flex-wrap: wrap; }
  .project-page .project-confirm { display: grid; gap: 8px; margin-top: 8px; padding: 10px 12px; border: 1px solid var(--line); border-radius: var(--r-lg); background: var(--sunken); }
  .project-page .project-confirm p { margin: 0; }
  .project-page .project-form { display: grid; gap: 8px; padding: 12px 16px; border-bottom: 1px solid var(--line); }
  .project-page .project-form p { margin: 0; }
  .project-page .project-form .row { gap: 8px; flex-wrap: wrap; }
  .project-page .project-basis { padding: 12px 16px; border-top: 1px solid var(--line); display: grid; gap: 6px; }
  .project-page .project-basis p { margin: 0; font-size: 12px; }
  .project-page .panel > .pb.project-empty { padding: 12px 16px; }
  .project-page .project-empty p { margin: 0; }
  .project-page .project-storage { display: grid; gap: 16px; }
  .project-page .project-storage p { margin: 0; }
  .project-page .activity-row { gap: 8px; min-width: 0; flex-wrap: nowrap; }
  .project-page .activity-age { width: 40px; flex: none; }
  .project-page .cell-claim { display: inline-flex; align-items: center; gap: 4px; padding-right: 4px; }
  .project-page .cell-input { border: 0; background: transparent; font: inherit; color: var(--ink); width: 96px; }
  .project-page .cell-conflict { display: flex; flex-wrap: wrap; gap: 4px; align-items: center; }
  .project-page .cell-conflict button.claim { cursor: pointer; font: inherit; }
  .project-page [data-add-results] li { gap: 8px; }
  /* P4 Task 4: Add to project on the run, Selection and item pages, a sample's row action, and the project tags. */
  .project-control[hidden], .project-control [hidden], .samples-projects[hidden] { display: none; }
  .project-control ul.plain { list-style: none; margin: 0; padding: 0; display: grid; gap: 8px; }
  .project-control p { margin: 0; }
  .project-control .row { gap: 8px; flex-wrap: wrap; }
  .project-control .project-control-form { display: grid; gap: 8px; max-width: 560px; }
  .project-control .project-control-form label { display: grid; gap: 4px; }
  .project-tags { display: inline-flex; flex-wrap: wrap; gap: 6px; }
  .samples-grid .cell-filter { display: inline-flex; vertical-align: middle; margin: 0 0 0 2px; padding: 2px; border-radius: 3px; flex: none;
    color: var(--ink-3); visibility: hidden; }
  .samples-grid tr:hover .cell-filter, .samples-grid .cell-filter:focus-visible { visibility: visible; }
  .samples-grid .cell-filter:hover { color: var(--accent-ink); background: var(--accent-soft); }
  /* P4 Task 5: the differentialabundance hand-off (mockup 14) and its rail. */
  .handoff-page { display: grid; gap: 16px; min-width: 0; }
  .handoff-page [hidden], .handoff [hidden] { display: none; }
  .handoff-page .cell-claim { display: inline-flex; align-items: center; gap: 4px; padding-right: 4px; }
  .handoff-page .cell-input { border: 0; background: transparent; font: inherit; color: var(--ink); width: 100px; min-height: 0; padding: 0; }
  .handoff-page .handoff-contrast { font-size: 13px; gap: 6px; flex-wrap: wrap; }
  .handoff-page .handoff-select { height: 30px; font: inherit; }
  .handoff-page .handoff-contrasts { font-size: 11.5px; white-space: pre-wrap; overflow-wrap: anywhere; margin: 0; }
  .handoff-page .handoff-checks { display: grid; gap: 8px; }
  .handoff-page .handoff-checks p { margin: 0; }
  .handoff-page .handoff-gtf { gap: 8px; margin-top: 8px; }
  .handoff-page .handoff-gtf input { flex: 1; min-width: 0; }
  .handoff-page #ss { border: 0; border-radius: 0; margin: 0; }
  .handoff .handoff-facts { gap: 6px; flex-wrap: wrap; }
  .handoff .handoff-saved, .handoff .handoff-keep { display: grid; gap: 6px; }
  .handoff .handoff-saved p, .handoff .handoff-keep p { margin: 0; }
  .handoff .handoff-saved .row, .handoff .handoff-add-project { gap: 6px; flex-wrap: wrap; }
  /* P4 Task 7: a tag is no wider than its box; a long value is cut with an ellipsis (claimTag titles it with the whole text). */
  .claim { max-width: 100%; min-width: 0; }
  .claim .verb, .claim .age { flex: none; }
  .claim .val { min-width: 0; overflow: hidden; text-overflow: ellipsis; }
  /* The rail's sections take its width and no more: a long tag or the run command can't widen it (pre.code scrolls sideways). */
  .rail #panel-inner { min-width: 0; }
  .rail .sec { grid-template-columns: minmax(0, 1fr); }
  /* --- P3 follow-up: the item page's Files table --- */
  /* Long file names and the "not stored here" reason no longer push Preview and Download past the panel at 1440 px: both wrap. */
  #files .namecell { white-space: normal; }
  #files .namecell > a, #files .namecell > span:not(.seal) { min-width: 0; overflow-wrap: anywhere; }
  #files td.nowrap > span.muted { white-space: normal; }
  /* --- end P3 follow-up --- */
`
