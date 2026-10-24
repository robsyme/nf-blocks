// web/src/styles.js
// The page's stylesheet (explorer layout B spec §7): tokens for light and dark,
// the three-region layout, and the narrow-screen drawer and sheet (spec §3.1).
// app.js appends it to <head> at start, beside pairs.js's PAIRS_CSS.
export const APP_CSS = `
  :root { color-scheme: light dark; --fg: #1d1d1f; --bg: #ffffff; --side: #f6f7f9; --muted: #6e6e73; --line: #d2d2d7;
    --accent: #2563eb; --accent-fg: #ffffff; --good: #15803d; --warn: #9a5b00; --bad: #b3261e; --num: #0550ae; --true: #116329;
    --false: #cf222e; --pill-key: #f6f8fa; --chip: #eef1ff; --code: #f6f8fa;
    --pill-line: #a9a9b0; --tip-bg: #1d1d1f; --tip-fg: #f5f5f7; }
  @media (prefers-color-scheme: dark) { :root { --fg: #f5f5f7; --bg: #161617; --side: #1d1d1f; --muted: #a1a1a6; --line: #3a3a3c;
    --accent: #6ea8ff; --accent-fg: #0b1220; --good: #56d364; --warn: #ffd180; --bad: #ff8a80; --num: #79c0ff; --true: #56d364;
    --false: #ff7b72; --pill-key: #2c2c2e; --chip: #2a2f45; --code: #1f1f22;
    --pill-line: #6b6b70; --tip-bg: #f5f5f7; --tip-fg: #1d1d1f; } }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.45 system-ui, sans-serif; color: var(--fg); background: var(--bg); }
  a { color: var(--accent); } h1 { font-size: 1.35rem; margin: .4rem 0 .2rem; } h2 { font-size: 1.02rem; }
  code, .cid { font: 12px ui-monospace, SFMono-Regular, Menlo, monospace; overflow-wrap: anywhere; }
  button { font: inherit; } button.primary { background: var(--accent); color: var(--accent-fg); border: 1px solid var(--accent); border-radius: 4px; padding: .15rem .6rem; }
  #bar { position: sticky; top: 0; z-index: 4; display: flex; gap: 1rem; align-items: center; padding: .5rem 16px;
    background: var(--bg); border-bottom: 1px solid var(--line); }
  #bar .brand { font-weight: 600; text-decoration: none; } #bar .spacer { flex: 1; }
  #nav-toggle, #use-toggle { display: none; }
  #shell { display: grid; grid-template-columns: 15rem minmax(0, 1fr) 21rem; min-height: calc(100vh - 2.7rem); }
  #nav { background: var(--side); border-right: 1px solid var(--line); padding: .75rem; overflow-y: auto; }
  #nav input[type=search] { width: 100%; }
  #nav ul { list-style: none; padding: 0; margin: .25rem 0; } #nav li { padding: .1rem 0; }
  #nav .nav-runs { padding-left: .9rem; }
  #nav .nav-run { display: flex; align-items: center; min-width: 0; } #nav .nav-run-name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; } #nav .nav-run-when { flex: none; margin-left: .4rem; white-space: nowrap; } #nav a { color: inherit; } #nav [aria-current=page] > a { font-weight: 600; color: var(--accent); }
  #nav .nav-toggle { background: none; border: 0; padding: 0; cursor: pointer; text-align: left; }
  .nav-head { font-size: .75rem; letter-spacing: .05em; text-transform: uppercase; color: var(--muted); margin: .9rem 0 .2rem; }
  #store-status { margin-top: 1.5rem; font-size: 12px; } #store-status > * { margin: .2rem 0; }
  #members a { margin-right: .75rem; }
  #main { padding: 0 16px 4rem; min-width: 0; }
  #panel { background: var(--side); border-left: 1px solid var(--line); padding: .75rem; }
  #panel-inner { position: sticky; top: 3.3rem; }
  .crumbs { color: var(--muted); margin-top: .75rem; } .crumbs a { color: inherit; }
  .muted { color: var(--muted); } [data-error] { color: var(--bad); } .warn, #stale[data-notice] { color: var(--warn); }
  table { border-collapse: collapse; width: 100%; } td, th { text-align: left; padding: .3rem .5rem; border-bottom: 1px solid var(--line); vertical-align: top; }
  th { color: var(--muted); font-weight: 500; font-size: 12px; } .scroll { overflow-x: auto; } td.output-name { white-space: nowrap; } tr.empty-output { opacity: .5; }
  .btn { display: inline-flex; align-items: center; gap: 6px; height: 28px; padding: 0 10px; border-radius: 6px; border: 1px solid var(--line); background: var(--bg); color: var(--fg); font: 500 13px system-ui, sans-serif; cursor: pointer; text-decoration: none; white-space: nowrap; } .btn:hover { border-color: var(--pill-line); background: var(--side); } .btn:focus-visible { outline: 2px solid color-mix(in srgb, var(--accent) 45%, transparent); outline-offset: 1px; } .btn:disabled { opacity: .55; cursor: default; } .btn-primary { background: var(--accent); border-color: var(--accent); color: var(--accent-fg); } .btn-primary:hover { background: var(--accent); filter: brightness(1.08); } .btn-ghost { border-color: transparent; background: none; color: var(--accent); padding: 0 6px; } .btn-ghost:hover { border-color: transparent; background: color-mix(in srgb, var(--accent) 10%, transparent); } .btn-icon { font-size: 15px; line-height: 1; }
  .fb-bar { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin: .4rem 0 .6rem; } .fb-spacer { flex: 1; } .fb-status { font-size: 13px; } .fb-anchor { position: relative; display: inline-block; } .fb-chip { display: inline-flex; align-items: center; max-width: 100%; height: 28px; border: 1px solid color-mix(in srgb, var(--accent) 35%, var(--line)); background: var(--chip); border-radius: 6px; font-size: 13px; overflow: hidden; } .fb-chip-key { padding: 0 2px 0 9px; color: var(--muted); font: 12px ui-monospace, SFMono-Regular, Menlo, monospace; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 26em; direction: rtl; text-align: left; } .fb-chip-op { color: var(--muted); padding: 0 4px; white-space: pre; } .fb-chip-val { padding: 0 6px 0 0; font-weight: 600; white-space: nowrap; } .fb-chip-x { display: inline-flex; align-items: center; justify-content: center; align-self: stretch; width: 26px; border-left: 1px solid color-mix(in srgb, var(--accent) 25%, var(--line)); color: var(--muted); text-decoration: none; font-size: 15px; } .fb-chip-x:hover { color: var(--fg); background: color-mix(in srgb, var(--accent) 12%, transparent); }
  .fb-picker { position: absolute; z-index: 15; top: 34px; left: 0; width: 26rem; max-width: 80vw; background: var(--bg); border: 1px solid var(--line); border-radius: 10px; box-shadow: 0 8px 24px #0002; padding: 8px; } .fb-picker[data-open=false] { display: none; } .fb-input { box-sizing: border-box; width: 100%; height: 30px; border: 1px solid var(--line); border-radius: 6px; padding: 0 9px; font: 13px system-ui, sans-serif; background: var(--bg); color: var(--fg); } .fb-input:focus { outline: 2px solid color-mix(in srgb, var(--accent) 45%, transparent); border-color: var(--accent); } .fb-list { max-height: 16rem; overflow: auto; margin-top: 6px; } .fb-item { display: flex; justify-content: space-between; gap: 12px; width: 100%; padding: 6px 8px; border: 0; border-radius: 6px; background: none; color: var(--fg); font: 13px system-ui, sans-serif; text-align: left; text-decoration: none; cursor: pointer; } .fb-item:hover, .fb-item:focus-visible, .fb-item.active { background: var(--side); outline: none; } .fb-path { font: 12px ui-monospace, SFMono-Regular, Menlo, monospace; word-break: break-all; } .fb-count { color: var(--muted); font-size: 12px; white-space: nowrap; } .fb-head { display: flex; align-items: center; gap: 6px; padding: 0 2px 6px; } .fb-back { height: 22px; font-size: 16px; } .fb-type { font: 11px ui-monospace, monospace; color: var(--muted); border: 1px solid var(--line); border-radius: 4px; padding: 0 4px; } .fb-ops { display: inline-flex; margin: 0 0 6px; border: 1px solid var(--line); border-radius: 6px; overflow: hidden; } .fb-op { min-width: 34px; height: 26px; border: 0; border-left: 1px solid var(--line); background: var(--bg); color: var(--fg); font: 12px ui-monospace, SFMono-Regular, Menlo, monospace; cursor: pointer; } .fb-op:first-child { border-left: 0; } .fb-op:hover { background: var(--side); } .fb-op[aria-pressed=true] { background: var(--accent); color: var(--accent-fg); } .fb-error { color: var(--bad); font-size: 12px; margin: 4px 2px 0; } .fb-error:empty { display: none; } .fb-note { margin: 8px 4px; font-size: 13px; }
  .file-pill { display: inline-flex; align-items: center; justify-content: center; box-sizing: border-box; width: 26px; height: 18px; border: 1px solid var(--pill-line); border-radius: 3px; margin-right: 4px; vertical-align: middle; color: var(--muted); text-decoration: none; } .file-pill:hover, .file-pill:focus-visible { border-color: var(--accent); color: var(--accent); background: color-mix(in srgb, var(--accent) 12%, transparent); outline: none; } .file-pill .icon { margin: 0; width: 12px; height: 12px; vertical-align: 0; pointer-events: none; } .file-pill:hover .icon, .file-pill:focus-visible .icon { background-color: var(--accent); } .file-pill-empty { border-style: dashed; opacity: .6; cursor: default; font: 10px ui-monospace, monospace; } .file-pill-more { font-size: 11px; color: var(--muted); margin: 0 6px 0 2px; text-decoration: none; } .file-pill-more:hover { color: var(--accent); }
  .items-table td, .items-table th { white-space: nowrap; vertical-align: middle; } .items-table th.file-key { font: 600 12px ui-monospace, SFMono-Regular, Menlo, monospace; color: var(--fg); } .items-table td.file-cell { background: color-mix(in srgb, var(--code) 70%, transparent); } .items-table .file-last { border-right: 1px solid var(--line); }
  button.file-pill { background: none; padding: 0; font: 600 13px/1 system-ui, sans-serif; cursor: pointer; } .file-pill.picked, .file-pill.picked:hover { border-color: var(--good); color: var(--good); background: color-mix(in srgb, var(--good) 10%, transparent); cursor: default; } .items-table .item-actions { border-right: 1px solid var(--line); }
  .file-pill-tip { position: fixed; z-index: 20; pointer-events: none; background: var(--tip-bg); color: var(--tip-fg); border-radius: 6px; padding: 6px 9px; font-size: 12px; line-height: 1.4; max-width: 420px; box-shadow: 0 4px 14px #0003; } .file-pill-tip[data-open=false] { display: none; } .file-pill-tip .tip-path { font: 600 12px ui-monospace, SFMono-Regular, Menlo, monospace; } .file-pill-tip .tip-name { opacity: .85; word-break: break-all; } .file-pill-tip .tip-size { opacity: .7; } .file-pill-tip .tip-reason { font: 11px ui-monospace, monospace; opacity: .85; }
  .ext { display: inline-block; background: var(--code); border-radius: 3px; padding: 0 4px; margin-right: 2px; font: 11px ui-monospace, monospace; } .index-link { margin-left: .35rem; } button.link { background: none; border: 0; padding: 0; color: var(--accent); text-decoration: underline; cursor: pointer; }
  .address { color: var(--muted); font: 12px ui-monospace, SFMono-Regular, Menlo, monospace; text-decoration-color: var(--line); white-space: nowrap; }
  .address .icon { margin-right: 2px; }
  .constant { font-size: 12px; } .badge { display: inline-block; border: 1px solid var(--line); border-radius: 999px; padding: .05rem .6rem; font-size: 12px; margin-right: .35rem; }
  details.fold { border-top: 1px solid var(--line); padding: .4rem 0; } .pills + details.fold { margin-top: .4rem; } details.fold > summary { cursor: pointer; font-weight: 600; }
  form { display: grid; gap: .5rem; margin: 1rem 0; } fieldset { border: 1px solid var(--line); }
  .rows { list-style: none; padding: 0; margin: .5rem 0; } .row { padding: .4rem 0; border-bottom: 1px solid var(--line); }
  .row-head { display: flex; flex-wrap: wrap; gap: .4rem; align-items: baseline; } .row-action { margin-left: auto; }
  .row-sub { margin: .15rem 0 0 1.6rem; color: var(--muted); } .row-sub code { color: var(--fg); }
  button.quiet { font-size: 12px; padding: 0 .35rem; }
  .row-actions { display: flex; flex-wrap: wrap; gap: .5rem; align-items: baseline; }
  .snippet { display: flex; gap: .5rem; align-items: flex-start; margin: .35rem 0; }
  .snippet pre { margin: 0; padding: .4rem .6rem; border: 1px solid var(--line); background: var(--bg); flex: 1; white-space: pre-wrap; overflow-wrap: anywhere; }
  .snippet-toggle button[aria-pressed=true], .run-switch button[aria-pressed=true] { font-weight: 600; }
  .picked, .picked-entries { list-style: none; padding: 0; } .picked-entries { margin-left: 1rem; font-size: 12px; }
  @media (max-width: 1200px) {
    #shell { grid-template-columns: minmax(0, 1fr) 21rem; }
    #nav { display: none; position: fixed; top: 2.7rem; bottom: 0; left: 0; width: 16rem; z-index: 3; }
    body[data-nav=open] #nav { display: block; } #nav-toggle { display: inline-block; }
  }
  @media (max-width: 900px) {
    #shell { grid-template-columns: minmax(0, 1fr); }
    #panel { display: none; position: fixed; left: 0; right: 0; bottom: 0; max-height: 70vh; overflow-y: auto; z-index: 3;
      border-left: 0; border-top: 1px solid var(--line); }
    #panel-inner { position: static; }
    body[data-panel=open] #panel { display: block; } #use-toggle { display: inline-block; }
  }
  .block-cid { font-size: 1rem; display: flex; gap: .4rem; align-items: baseline; flex-wrap: wrap; } .block-cid code { overflow-wrap: anywhere; }
  .block-parts { display: grid; grid-template-columns: auto 1fr; gap: .1rem .8rem; } .block-parts dt { color: var(--muted); } .block-parts dd { margin: 0; }
  .ok { color: var(--good); } pre.dag-json { background: var(--code); border-radius: 5px; padding: .5rem .7rem; overflow-x: auto; }
`
