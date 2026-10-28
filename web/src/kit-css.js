// web/src/kit-css.js
// The Blockprint kit's tokens and component classes, ported from
// docs/design/mockups/src/kit.css (the source of truth: change it there
// first). Left out: the mockup chrome (.spec*, .theme-fab, body.show-pins),
// the Google Fonts import (the page fetches nothing; the font stacks fall back
// to system fonts) and the kit shell's two breakpoints, which styles.js
// replaces with the explorer's drawer and sheet. One change: .picked is
// scoped to the top bar, since the Use panel's list and the items grid's
// pills also use that class. app.js injects KIT_CSS before APP_CSS.
export const KIT_CSS = `
:root{
  /* Seqera base */
  --bg:#f7f7fa; --surface:#ffffff; --sunken:#f0f0f5; --grid:rgba(32,22,55,.04);
  --ink:#201637; --ink-2:#4d4566; --ink-3:#736c89; --line:#e2e0ea; --line-2:#cbc8d8;
  --accent:#31c9ac; --accent-ink:#065647; --accent-soft:#e2f7f3; --on-accent:#201637;
  /* Stores: categorical, differ in lightness */
  --store-a:#6a55d8; --store-b:#1f7fbf; --store-c:#b8487f;
  /* Durability: semantic */
  --pinned:#0b7a63; --pinned-bg:#e2f4ef;
  --held:#6e6787;   --held-bg:#eeedf3;
  --releasing:#a86400; --releasing-bg:#fbf0de;
  --missing:#a23b3b; --missing-bg:#f8e6e6;
  /* Status */
  --ok:#0b7a63; --fail:#b42318;
  /* Claims */
  --claim-bg:#fff8e8; --claim-line:#e2c98a;
  /* Type */
  --font-ui:"Inter",ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;
  --font-mono:"JetBrains Mono",ui-monospace,SFMono-Regular,Menlo,monospace;
  --fs-xs:11.5px; --fs-sm:12.5px; --fs-md:13.5px; --fs-lg:15px; --fs-title:22px;
  /* Shape */
  --r-sm:4px; --r-md:6px; --r-lg:10px;
  --shadow:0 1px 2px rgba(32,22,55,.06),0 6px 20px rgba(32,22,55,.06);
  --shadow-pop:0 12px 32px rgba(32,22,55,.16);
  /* Shell */
  --top-h:52px; --nav-w:232px; --rail-w:320px;
}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){
  --bg:#130e22; --surface:#1b1530; --sunken:#110d1f; --grid:rgba(220,210,255,.035);
  --ink:#eeebf6; --ink-2:#c3bdd6; --ink-3:#9790b0; --line:#2f2747; --line-2:#433a60;
  --accent:#31c9ac; --accent-ink:#5fe0c6; --accent-soft:#163a35; --on-accent:#130e22;
  --store-a:#a596ff; --store-b:#5db3ea; --store-c:#e583b3;
  --pinned:#45d6b8; --pinned-bg:#123630; --held:#a49ebb; --held-bg:#262040;
  --releasing:#f0a640; --releasing-bg:#3a2a10; --missing:#ef8f8f; --missing-bg:#3a1a1e;
  --ok:#45d6b8; --fail:#ff8a80; --claim-bg:#2c2514; --claim-line:#6e5a2a;
  --shadow:0 1px 2px rgba(0,0,0,.3),0 6px 20px rgba(0,0,0,.25); --shadow-pop:0 12px 32px rgba(0,0,0,.5);
  color-scheme:dark}}
:root[data-theme="dark"]{
  --bg:#130e22; --surface:#1b1530; --sunken:#110d1f; --grid:rgba(220,210,255,.035);
  --ink:#eeebf6; --ink-2:#c3bdd6; --ink-3:#9790b0; --line:#2f2747; --line-2:#433a60;
  --accent:#31c9ac; --accent-ink:#5fe0c6; --accent-soft:#163a35; --on-accent:#130e22;
  --store-a:#a596ff; --store-b:#5db3ea; --store-c:#e583b3;
  --pinned:#45d6b8; --pinned-bg:#123630; --held:#a49ebb; --held-bg:#262040;
  --releasing:#f0a640; --releasing-bg:#3a2a10; --missing:#ef8f8f; --missing-bg:#3a1a1e;
  --ok:#45d6b8; --fail:#ff8a80; --claim-bg:#2c2514; --claim-line:#6e5a2a;
  --shadow:0 1px 2px rgba(0,0,0,.3),0 6px 20px rgba(0,0,0,.25); --shadow-pop:0 12px 32px rgba(0,0,0,.5);
  color-scheme:dark}

*{box-sizing:border-box}
html,body{margin:0}
body{background:var(--bg);color:var(--ink);font-family:var(--font-ui);font-size:var(--fs-md);line-height:1.5;-webkit-font-smoothing:antialiased}
h1,h2,h3,h4{margin:0;text-wrap:balance;letter-spacing:-.015em}
p{margin:0}
a{color:var(--accent-ink);text-decoration:none}
a:hover{text-decoration:underline}
button{font:inherit;color:inherit}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-radius:2px}
.mono{font-family:var(--font-mono);font-size:.9em}
.muted{color:var(--ink-3)}
.num{font-variant-numeric:tabular-nums}
.eyebrow{font-family:var(--font-mono);font-size:10.5px;letter-spacing:.08em;text-transform:uppercase;color:var(--ink-3)}
.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.stack{display:grid;gap:12px;align-content:start;min-width:0}
.spacer{flex:1}

/* ---------- App shell ---------- */
.app{min-height:100vh;display:grid;grid-template-columns:var(--nav-w) minmax(0,1fr);grid-template-rows:var(--top-h) 1fr}
.app.has-rail{grid-template-columns:var(--nav-w) minmax(0,1fr) var(--rail-w)}
.top{grid-column:1/-1;position:sticky;top:0;z-index:30;display:flex;align-items:center;gap:14px;padding:0 16px;background:var(--surface);border-bottom:1px solid var(--line)}
.brand{display:flex;gap:8px;align-items:center;font-weight:700;letter-spacing:-.01em;color:var(--ink);font-size:14.5px}
.store-switch{display:inline-flex;align-items:center;gap:8px;height:32px;padding:0 10px;border:1px solid var(--line);border-radius:var(--r-md);background:var(--surface);font-size:var(--fs-sm);cursor:pointer}
.store-switch .chev{color:var(--ink-3)}
.search{flex:1;max-width:520px;display:flex;align-items:center;gap:8px;height:32px;padding:0 10px;border:1px solid var(--line);border-radius:var(--r-md);background:var(--sunken);color:var(--ink-3);font-size:var(--fs-sm)}
.search input{flex:1;border:0;background:transparent;font:inherit;color:var(--ink);outline:none;min-width:0}
.kbd{font-family:var(--font-mono);font-size:10.5px;padding:1px 5px;border:1px solid var(--line-2);border-radius:3px;color:var(--ink-3)}
.top .right{margin-left:auto;display:flex;gap:10px;align-items:center}
.top .picked{display:inline-flex;align-items:center;gap:6px;height:32px;padding:0 12px;border-radius:var(--r-md);border:1px solid var(--line);background:var(--surface);font-size:var(--fs-sm);font-weight:600;cursor:pointer}
.top .picked .count{min-width:18px;height:18px;border-radius:9px;background:var(--accent);color:var(--on-accent);font-size:11px;display:inline-grid;place-items:center;padding:0 5px}
.me{display:inline-flex;gap:8px;align-items:center;font-size:var(--fs-sm);color:var(--ink-2)}

.nav{border-right:1px solid var(--line);background:var(--bg);padding:14px 10px 20px;display:flex;flex-direction:column;gap:2px;position:sticky;top:var(--top-h);height:calc(100vh - var(--top-h));overflow-y:auto}
.nav .h{padding:14px 8px 4px}
.nav a{display:flex;align-items:center;gap:8px;padding:5px 8px;border-radius:var(--r-sm);color:var(--ink-2);font-size:var(--fs-sm);text-decoration:none;min-width:0}
.nav a .lbl{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.nav a small{color:var(--ink-3);font-size:11px;font-variant-numeric:tabular-nums}
.nav a:hover{background:var(--sunken)}
.nav a.on{background:var(--accent-soft);color:var(--accent-ink);font-weight:600}
.nav a.sub{padding-left:24px}
.nav a.sub.on{background:transparent;color:var(--ink);font-weight:600;box-shadow:inset 2px 0 0 var(--ink)}
.nav .foot{margin-top:auto;padding:12px 8px 0;font-size:11.5px;color:var(--ink-3);display:grid;gap:3px;border-top:1px solid var(--line)}
.nav-ico{width:16px;height:16px;flex:none;stroke:currentColor;fill:none;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}

.main{padding:20px 28px 48px;display:grid;gap:18px;align-content:start;min-width:0;
  background-image:linear-gradient(var(--grid) 1px,transparent 1px),linear-gradient(90deg,var(--grid) 1px,transparent 1px);background-size:24px 24px}
.rail{border-left:1px solid var(--line);background:var(--bg);padding:18px 16px;display:grid;gap:16px;align-content:start;position:sticky;top:var(--top-h);height:calc(100vh - var(--top-h));overflow-y:auto;min-width:0}
.rail h3{font-size:var(--fs-md);font-weight:650}
.rail .sec{display:grid;gap:8px}
.rail .sec + .sec{padding-top:14px;border-top:1px solid var(--line)}

/* ---------- Page header ---------- */
.crumbs{font-size:var(--fs-sm);color:var(--ink-3);display:flex;gap:6px;flex-wrap:wrap}
.crumbs a{color:var(--ink-3)} .crumbs b{color:var(--ink-2);font-weight:500}
.page-head{display:grid;gap:8px}
.title{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.title h1{font-size:var(--fs-title);font-weight:650;letter-spacing:-.02em}
.subline{display:flex;gap:6px 14px;flex-wrap:wrap;font-size:var(--fs-sm);color:var(--ink-3)}
.subline b{color:var(--ink-2);font-weight:500}
.tabs{display:flex;gap:2px;border-bottom:1px solid var(--line);overflow-x:auto}
.tabs a{padding:8px 12px;font-size:var(--fs-md);color:var(--ink-3);border-bottom:2px solid transparent;margin-bottom:-1px;white-space:nowrap;text-decoration:none}
.tabs a:hover{color:var(--ink)}
.tabs a.on{color:var(--ink);border-color:var(--ink);font-weight:600}
.tabs a small{font-variant-numeric:tabular-nums;color:var(--ink-3);margin-left:4px;font-weight:400}

/* ---------- Panels ---------- */
.panel{background:var(--surface);border:1px solid var(--line);border-radius:var(--r-lg);min-width:0}
.panel > .ph{display:flex;align-items:center;gap:10px;padding:12px 16px;border-bottom:1px solid var(--line)}
.panel > .ph h2{font-size:var(--fs-lg);font-weight:650}
.panel > .pb{padding:14px 16px;display:grid;gap:12px;min-width:0}
.panel.flush > .pb{padding:0}
.grid-2{display:grid;grid-template-columns:minmax(0,1.5fr) minmax(0,1fr);gap:18px;align-items:start}
.opt{display:grid;grid-template-columns:auto minmax(0,1fr) auto;gap:8px;align-items:center;font-size:var(--fs-sm);padding:7px 9px;border:1px solid var(--line);border-radius:var(--r-md);background:var(--surface);cursor:pointer}
.opt .d{color:var(--ink-3);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.opt .sz{font-family:var(--font-mono);font-size:11px;color:var(--ink-3);white-space:nowrap}
.opt .note2{grid-column:2/-1;font-size:11.5px;color:var(--ink-3)}
.check{display:grid;grid-template-columns:auto minmax(0,1fr);gap:8px;align-items:start;font-size:var(--fs-sm)}
.grid-3{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}
@media (max-width:1180px){.grid-2,.grid-3{grid-template-columns:minmax(0,1fr)}}

/* ---------- Tables ---------- */
.tbox{overflow-x:auto;min-width:0}
table.t{width:100%;border-collapse:collapse;font-size:var(--fs-md)}
.t th{text-align:left;font-weight:500;color:var(--ink-3);font-size:var(--fs-xs);padding:8px 10px;border-bottom:1px solid var(--line);white-space:nowrap;vertical-align:bottom;background:var(--surface);position:sticky;top:0}
.t th small{display:block;font-family:var(--font-mono);font-size:10px;font-weight:400;opacity:.85}
.t td{padding:9px 10px;border-bottom:1px solid var(--line);vertical-align:middle}
.t tr:last-child td{border-bottom:0}
.t td.r,.t th.r{text-align:right}
.t td.nowrap{white-space:nowrap}
.t tbody tr:hover td{background:color-mix(in srgb,var(--sunken) 70%,transparent)}
.t tr.sel td{background:var(--accent-soft)}
.t tr.dim td{opacity:.4}
.t tr.group td{background:var(--sunken);font-size:var(--fs-sm);color:var(--ink-3);padding:7px 10px}
.t .namecell{display:flex;align-items:center;gap:8px;min-width:0;white-space:nowrap}
.t .namecell a{font-weight:600}
.t .sub{font-size:var(--fs-xs);color:var(--ink-3)}
.bar-cell{display:flex;align-items:center;gap:8px;justify-content:flex-end}
.minibar{width:64px;height:6px;border-radius:3px;background:var(--sunken);border:1px solid var(--line);overflow:hidden;flex:none}
.minibar span{display:block;height:100%;background:var(--ink-3)}

/* ---------- Seal + address ---------- */
.seal{display:inline-block;vertical-align:middle;flex:none;line-height:0}
.seal svg{display:block}
.addr{display:inline-flex;align-items:center;gap:6px;font-family:var(--font-mono);font-size:11.5px;color:var(--ink-2);padding:2px 8px 2px 3px;border:1px solid var(--line);border-radius:var(--r-sm);background:var(--surface);white-space:nowrap;cursor:pointer}
.addr:hover{border-color:var(--line-2)}
.addr .copy{color:var(--ink-3);font-size:10px}

/* ---------- Fact + claim ---------- */
.fact{display:inline-flex;align-items:center;gap:8px;padding:4px 10px 4px 5px;border:1px solid var(--line-2);border-radius:var(--r-md);background:var(--surface);font-size:var(--fs-sm);white-space:nowrap}
.fact .k{font-family:var(--font-mono);font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:var(--ink-3)}
.claim{display:inline-flex;align-items:center;gap:7px;padding:3px 10px 3px 7px;background:var(--claim-bg);border:1px solid var(--claim-line);border-radius:var(--r-sm) var(--r-md) var(--r-md) var(--r-sm);font-size:var(--fs-sm);white-space:nowrap;color:var(--ink)}
.claim::before{content:"";width:6px;height:6px;border-radius:50%;flex:none;background:var(--bg);border:1.5px solid var(--claim-line)}
.claim .verb{font-weight:600}
.claim .age{color:var(--ink-3);font-size:var(--fs-xs)}
.claim.conflict{border-color:var(--releasing);background:var(--releasing-bg)}
.av{display:inline-grid;place-items:center;width:20px;height:20px;border-radius:50%;font-size:9.5px;font-weight:700;color:#fff;background:var(--ink-2);flex:none;letter-spacing:0}
.av.sm{width:16px;height:16px;font-size:8px}
.av.lg{width:28px;height:28px;font-size:11px}
.av.rs{background:var(--store-a)} .av.ak{background:var(--store-b)} .av.jt{background:var(--store-c)} .av.anon{background:transparent;color:var(--ink-3);border:1.5px dashed var(--ink-3)}

/* ---------- Presence + durability + status ---------- */
.presence{display:inline-flex;gap:3px;align-items:center}
.dot{width:10px;height:10px;border-radius:50%;display:inline-block;flex:none}
.dot.a{background:var(--store-a)} .dot.b{background:var(--store-b)} .dot.c{background:var(--store-c)}
.dot.ring{background:transparent;border:2px solid currentColor}
.dot.ring.a{color:var(--store-a)} .dot.ring.b{color:var(--store-b)} .dot.ring.c{color:var(--store-c)}
.state{display:inline-flex;align-items:center;gap:6px;font-size:var(--fs-xs);font-weight:600;padding:2px 8px;border-radius:999px;white-space:nowrap}
.state::before{content:"";width:7px;height:7px;border-radius:50%;background:currentColor;flex:none}
.s-pinned{color:var(--pinned);background:var(--pinned-bg)}
.s-held{color:var(--held);background:var(--held-bg)}
.s-releasing{color:var(--releasing);background:var(--releasing-bg)}
.s-missing{color:var(--missing);background:var(--missing-bg)}
.s-missing::before{background:transparent;border:1.5px solid currentColor}
.status{display:inline-flex;align-items:center;gap:6px;font-size:var(--fs-sm);font-weight:500;white-space:nowrap}
.status svg{width:14px;height:14px}
.status.ok{color:var(--ok)} .status.fail{color:var(--fail)}

/* ---------- Chips ---------- */
.kind{display:inline-flex;align-items:center;gap:4px;font-family:var(--font-mono);font-size:10.5px;padding:1px 6px;border-radius:3px;border:1px solid var(--line);color:var(--ink-2);background:var(--surface);white-space:nowrap}
.kind b{font-weight:500;color:var(--ink-3)}
.kinds{display:flex;flex-wrap:wrap;gap:4px}
.field{display:inline-flex;align-items:baseline;gap:6px;padding:3px 8px;border:1px solid var(--line);border-radius:var(--r-sm);background:var(--surface);font-size:var(--fs-sm);white-space:nowrap}
.field .k{color:var(--ink-3);font-size:var(--fs-xs)}
.field .v{font-weight:500;font-variant-numeric:tabular-nums}
.fileb{display:inline-flex;align-items:center;gap:5px;padding:2px 7px 2px 3px;border:1px solid var(--line);border-radius:var(--r-sm);background:var(--surface);font-size:11.5px;white-space:nowrap;color:var(--ink-2);text-decoration:none}
.fileb:hover{border-color:var(--line-2);text-decoration:none}
.fileb .ext{font-family:var(--font-mono);font-size:10px;color:var(--ink-3)}
.more{font-size:11.5px;color:var(--ink-3);padding:2px 4px}

/* ---------- Filters ---------- */
.filterbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.fchip{display:inline-flex;align-items:center;gap:6px;height:30px;padding:0 4px 0 10px;border:1px solid var(--line-2);border-radius:var(--r-md);background:var(--surface);font-size:var(--fs-sm)}
.fchip .src{font-family:var(--font-mono);font-size:10px;color:var(--ink-3)}
.fchip .op{font-family:var(--font-mono);color:var(--ink-3)}
.fchip input{width:84px;font:inherit;font-family:var(--font-mono);font-size:var(--fs-sm);padding:2px 6px;border:1px solid var(--line);border-radius:4px;background:var(--sunken);color:var(--ink)}
.fchip .x{width:22px;height:22px;border:0;background:transparent;color:var(--ink-3);border-radius:4px;cursor:pointer}
.fchip .x:hover{background:var(--sunken);color:var(--ink)}
.constants{display:flex;gap:6px;flex-wrap:wrap;align-items:center;font-size:var(--fs-sm);color:var(--ink-3)}

/* ---------- Buttons ---------- */
.btn{display:inline-flex;align-items:center;justify-content:center;gap:6px;height:32px;padding:0 12px;font-size:var(--fs-sm);font-weight:600;border-radius:var(--r-md);border:1px solid var(--line-2);background:var(--surface);color:var(--ink);cursor:pointer;white-space:nowrap;text-decoration:none}
.btn:hover{border-color:var(--ink-3);text-decoration:none}
.btn.primary{background:var(--accent);border-color:var(--accent);color:var(--on-accent)}
.btn.primary:hover{filter:brightness(.96)}
.btn.ghost{background:transparent;border-color:transparent;color:var(--ink-2);font-weight:500}
.btn.ghost:hover{background:var(--sunken)}
.btn.danger{color:var(--releasing);border-color:var(--releasing)}
.btn.sm{height:26px;padding:0 9px;font-size:var(--fs-xs)}
.btn[disabled]{opacity:.45;cursor:not-allowed}
.iconbtn{width:28px;height:28px;display:inline-grid;place-items:center;border:1px solid var(--line);border-radius:var(--r-sm);background:var(--surface);color:var(--ink-2);cursor:pointer;font-size:12px}

/* ---------- Code + notes ---------- */
pre.code{margin:0;font-family:var(--font-mono);font-size:11.5px;line-height:1.55;background:var(--sunken);border:1px solid var(--line);border-radius:var(--r-md);padding:10px 12px;overflow-x:auto;white-space:pre;color:var(--ink-2)}
pre.code .k{color:var(--accent-ink)} pre.code .s{color:var(--store-a)} pre.code .c{color:var(--ink-3)}
.seg-toggle{display:inline-flex;border:1px solid var(--line);border-radius:var(--r-md);overflow:hidden}
.seg-toggle button{border:0;background:var(--surface);padding:4px 10px;font-size:var(--fs-xs);color:var(--ink-3);cursor:pointer}
.seg-toggle a{display:inline-block;padding:4px 10px;font-size:var(--fs-xs);color:var(--ink-3);background:var(--surface);text-decoration:none}
.seg-toggle a.on{background:var(--ink);color:var(--surface);font-weight:600}
.seg-toggle button[disabled]{opacity:.45;cursor:not-allowed}
.seg-toggle button.on{background:var(--ink);color:var(--surface);font-weight:600}
.note{font-size:var(--fs-sm);padding:10px 12px;border-radius:var(--r-md);background:var(--sunken);color:var(--ink-2);display:flex;gap:8px;align-items:flex-start}
.note.warn{background:var(--releasing-bg);color:var(--releasing)}
.note.ok{background:var(--pinned-bg);color:var(--pinned)}
.note.bad{background:var(--missing-bg);color:var(--missing)}
.empty{display:grid;place-items:center;gap:6px;padding:28px;text-align:center;color:var(--ink-3);font-size:var(--fs-sm);border:1px dashed var(--line-2);border-radius:var(--r-lg)}
.empty b{color:var(--ink-2);font-size:var(--fs-md)}

/* ---------- Storage bar ---------- */
.sbar{display:grid;gap:7px;min-width:0}
.sbar .storehd{display:flex;gap:6px;align-items:center;font-size:var(--fs-sm);font-weight:600}
.sbar .track{display:flex;height:14px;border-radius:3px;overflow:hidden;background:var(--sunken);border:1px solid var(--line)}
.sbar .seg{height:100%}
.seg.pinned{background:var(--pinned)} .seg.held{background:var(--held);opacity:.55}
.seg.releasing{background:repeating-linear-gradient(135deg,var(--releasing) 0 4px,transparent 4px 7px)}
.sbar .legend{display:flex;flex-wrap:wrap;gap:4px 14px;font-size:var(--fs-xs);color:var(--ink-2)}
.sbar .legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px;vertical-align:-1px}
.sbar .mine{display:grid;grid-template-columns:40px 1fr auto;gap:8px;align-items:center;font-size:var(--fs-xs);color:var(--ink-2)}
.sbar .mine b{font-weight:600;color:var(--ink)}
.sbar .mine .mt{height:6px;border-radius:3px;background:var(--sunken);border:1px solid var(--line);overflow:hidden}
.sbar .mine .mt span{display:block;height:100%;background:var(--ink);opacity:.7}
.bignum{font-size:26px;font-weight:650;letter-spacing:-.02em;font-variant-numeric:tabular-nums}
.kv{display:grid;grid-template-columns:auto 1fr;gap:4px 14px;font-size:var(--fs-sm)}
.kv dt{color:var(--ink-3)} .kv dd{margin:0;font-variant-numeric:tabular-nums;min-width:0;overflow-wrap:anywhere}

/* ---------- Lineage thread ---------- */
.thread{display:grid;position:relative;padding-left:22px}
.thread::before{content:"";position:absolute;left:8px;top:14px;bottom:14px;width:2px;background:var(--line-2)}
.thread .node{position:relative;padding:6px 0;display:flex;gap:8px;align-items:center;flex-wrap:wrap;font-size:var(--fs-sm)}
.thread .node::before{content:"";position:absolute;left:-18px;top:50%;width:10px;height:10px;margin-top:-5px;border-radius:50%;background:var(--surface);border:2px solid var(--ink-3)}
.thread .node.here::before{background:var(--accent);border-color:var(--accent-ink)}
.thread .node.ext::before{border-style:dashed}

/* ---------- Lineage tree ---------- */
.tree{position:relative;padding-left:22px}
.tree::before{content:"";position:absolute;left:8px;top:16px;bottom:16px;width:2px;background:var(--line-2)}
.tree-group{position:relative;padding:4px 0 8px}
.tree-group + .tree-group{margin-top:4px}
.tree-head{position:relative;display:flex;gap:8px;align-items:center;flex-wrap:wrap;min-height:32px}
.tree-head::before,.tree-here::before{content:"";position:absolute;left:-18px;top:16px;width:10px;height:10px;margin-top:-5px;border-radius:50%;background:var(--surface);border:2px solid var(--ink-3)}
.tree-sub{display:flex;gap:6px 8px;align-items:center;flex-wrap:wrap;margin:4px 0 0 12px;font-size:var(--fs-sm)}
.tree-rows{display:grid;grid-template-columns:minmax(90px,max-content) minmax(140px,max-content) minmax(0,1fr) auto;column-gap:14px;margin:6px 0 0 12px;padding-left:16px;border-left:2px solid var(--line)}
.tree-rows.no-item{grid-template-columns:minmax(140px,max-content) minmax(0,1fr) auto}
.tree-rows.no-item .tree-item{display:none}
.tree-row{position:relative;grid-column:1/-1;display:grid;grid-template-columns:subgrid;column-gap:14px;align-items:center;padding:4px 0;font-size:var(--fs-sm)}
.tree-row::before{content:"";position:absolute;left:-16px;top:14px;width:10px;border-top:2px solid var(--line)}
.tree-row.ext::before{border-top-style:dashed}
.tree-file{font-family:var(--font-mono);font-size:12px}
.tree-row.ext .tree-file,.tree-row.ext .tree-file a{color:var(--ink-3)}
.tree-tag{display:flex;gap:5px;align-items:center}
.tree-size{color:var(--ink-3);font-variant-numeric:tabular-nums;font-size:12px;text-align:right}
.tree-rows > .fold{grid-column:1/-1;border-top:0;padding:0}
.tree-rows > .fold > summary{padding:4px 0;font-size:var(--fs-sm)}
.tree-rows > .fold[open] > summary{margin-bottom:0}
.tree-more{display:grid;grid-template-columns:minmax(90px,max-content) minmax(140px,max-content) minmax(0,1fr) auto;column-gap:14px}
.tree-rows.no-item .tree-more{grid-template-columns:minmax(140px,max-content) minmax(0,1fr) auto}
@supports selector(::details-content){.tree-rows > .fold,.tree-rows > .fold::details-content,.tree-rows > .fold .tree-more{display:grid;grid-template-columns:subgrid;grid-column:1/-1}.tree-rows > .fold > summary{grid-column:1/-1}}
.tree .also{display:inline-flex;gap:5px;align-items:center;font-size:11.5px;color:var(--ink-3);border:1px solid var(--line);border-radius:var(--r-sm);padding:1px 6px 1px 3px;white-space:nowrap}
.tree span.also{padding:1px 6px;cursor:help}
.tree .also .seal svg{display:block}
.tree-here{position:relative;display:flex;gap:8px;align-items:center;flex-wrap:wrap;padding:6px 0;font-size:var(--fs-sm)}
.tree-here::before{background:var(--accent);border-color:var(--accent-ink)}

/* ---------- Drawer / dialog ---------- */
.overlay{position:fixed;inset:0;background:rgba(19,14,34,.38);z-index:50;display:grid;place-items:center;padding:24px}
.dialog{width:min(560px,100%);background:var(--surface);border:1px solid var(--line);border-radius:var(--r-lg);box-shadow:var(--shadow-pop);display:grid;gap:0}
.dialog .dh{padding:16px 18px;border-bottom:1px solid var(--line);display:flex;gap:10px;align-items:center}
.dialog .dh h2{font-size:16px;font-weight:650}
.dialog .db{padding:16px 18px;display:grid;gap:12px}
.dialog .df{padding:12px 18px;border-top:1px solid var(--line);display:flex;gap:8px;justify-content:flex-end}
.inline-confirm{border:1px solid var(--releasing);border-radius:var(--r-lg);background:var(--surface);box-shadow:var(--shadow)}

@media (prefers-reduced-motion:no-preference){.t td{transition:background .12s ease,opacity .16s ease}}
`
