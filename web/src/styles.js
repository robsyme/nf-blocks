// web/src/styles.js
// The page's stylesheet (explorer layout B spec §7): tokens for light and dark,
// the three-region layout, and the narrow-screen drawer and sheet (spec §3.1).
// app.js appends it to <head> at start, beside pairs.js's PAIRS_CSS.
export const APP_CSS = `
  :root { color-scheme: light dark; --fg: #1d1d1f; --bg: #ffffff; --side: #f6f7f9; --muted: #6e6e73; --line: #d2d2d7;
    --accent: #2563eb; --accent-fg: #ffffff; --good: #15803d; --warn: #9a5b00; --bad: #b3261e; --num: #0550ae; --true: #116329;
    --false: #cf222e; --pill-key: #f6f8fa; --chip: #eef1ff; --code: #f6f8fa; }
  @media (prefers-color-scheme: dark) { :root { --fg: #f5f5f7; --bg: #161617; --side: #1d1d1f; --muted: #a1a1a6; --line: #3a3a3c;
    --accent: #6ea8ff; --accent-fg: #0b1220; --good: #56d364; --warn: #ffd180; --bad: #ff8a80; --num: #79c0ff; --true: #56d364;
    --false: #ff7b72; --pill-key: #2c2c2e; --chip: #2a2f45; --code: #1f1f22; } }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.45 system-ui, sans-serif; color: var(--fg); background: var(--bg); }
  a { color: inherit; } h1 { font-size: 1.35rem; margin: .4rem 0 .2rem; } h2 { font-size: 1.02rem; }
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
  #nav .nav-runs { padding-left: .9rem; } #nav [aria-current=page] > a { font-weight: 600; color: var(--accent); }
  #nav .nav-toggle { background: none; border: 0; padding: 0; cursor: pointer; text-align: left; }
  .nav-head { font-size: .75rem; letter-spacing: .05em; text-transform: uppercase; color: var(--muted); margin: .9rem 0 .2rem; }
  #store-status { margin-top: 1.5rem; font-size: 12px; } #store-status > * { margin: .2rem 0; }
  #members a { margin-right: .75rem; }
  #main { padding: 0 16px 4rem; min-width: 0; }
  #panel { background: var(--side); border-left: 1px solid var(--line); padding: .75rem; }
  #panel-inner { position: sticky; top: 3.3rem; }
  .dot-good { color: var(--good); } .dot-bad { color: var(--bad); }
  .crumbs { color: var(--muted); margin-top: .75rem; } .crumbs a { color: inherit; }
  .muted { color: var(--muted); } [data-error] { color: var(--bad); } .warn, #stale[data-notice] { color: var(--warn); }
  table { border-collapse: collapse; width: 100%; } td, th { text-align: left; padding: .3rem .5rem; border-bottom: 1px solid var(--line); vertical-align: top; }
  th { color: var(--muted); font-weight: 500; font-size: 12px; } .scroll { overflow-x: auto; }
  .chip { display: inline-block; background: var(--chip); border-radius: 4px; padding: 0 5px; margin: 0 2px 2px 0; font-size: 12px; }
  .ext { display: inline-block; background: var(--code); border-radius: 3px; padding: 0 4px; margin-right: 2px; font: 11px ui-monospace, monospace; }
  .constant { font-size: 12px; } .badge { display: inline-block; border: 1px solid var(--line); border-radius: 999px; padding: .05rem .6rem; font-size: 12px; margin-right: .35rem; }
  details.fold { border-top: 1px solid var(--line); padding: .4rem 0; } details.fold > summary { cursor: pointer; font-weight: 600; }
  form { display: grid; gap: .5rem; margin: 1rem 0; } fieldset { border: 1px solid var(--line); }
  .rows { list-style: none; padding: 0; margin: .5rem 0; } .row { padding: .4rem 0; border-bottom: 1px solid var(--line); }
  .row-head { display: flex; flex-wrap: wrap; gap: .4rem; align-items: baseline; } .row-action { margin-left: auto; }
  .row-pairs, .row-via, .row-cid { margin: .15rem 0 0 1.6rem; } .row-pairs:empty { display: none; }
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
`
