#!/usr/bin/env python3
"""Build standalone nf-blocks mockup pages.

Each pages/*.html partial starts with a <!--META {json} --> block, then
<!--MAIN--> content and optionally <!--RAIL--> content. This script wraps them
in the shared app shell and inlines kit.css + kit.js so every output file is
self-contained (only Google Fonts is fetched; system fonts are the fallback).
"""
import json, re, html, pathlib

SRC = pathlib.Path(__file__).parent
OUT = SRC.parent
CSS = (SRC / "kit.css").read_text()
JS = (SRC / "kit.js").read_text()

ICONS = {
  "projects": '<path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>',
  "pipelines": '<circle cx="6" cy="6" r="2.5"/><circle cx="18" cy="18" r="2.5"/><path d="M8.5 6H14a4 4 0 0 1 4 4v5.5"/>',
  "selections": '<path d="M7 3h10a1 1 0 0 1 1 1v17l-6-4-6 4V4a1 1 0 0 1 1-1z"/>',
  "storage": '<ellipse cx="12" cy="5.5" rx="7.5" ry="2.5"/><path d="M4.5 5.5v13c0 1.4 3.4 2.5 7.5 2.5s7.5-1.1 7.5-2.5v-13"/><path d="M4.5 12c0 1.4 3.4 2.5 7.5 2.5s7.5-1.1 7.5-2.5"/>',
  "search": '<circle cx="11" cy="11" r="6.5"/><path d="m20 20-4.2-4.2"/>',
}
def icon(name, cls="nav-ico"):
    return f'<svg class="{cls}" viewBox="0 0 24 24" aria-hidden="true">{ICONS[name]}</svg>'

STORES = {
  "a": ("roblab", "142 MB", "local · writable"),
  "b": ("org-s3", "1.82 TB", "s3 · writable"),
  "c": ("ana-hpc", "410 GB", "posix · read-only"),
}
RUNS = [("distraught_murdock", "Oct 24", "ok"), ("sad_yonath", "Oct 23", "ok"), ("sharp_magritte", "Oct 23", "fail")]

def top(meta):
    store = meta.get("store", "roblab")
    stores = meta.get("stores", ["a"])
    if store == "all":
        sw = '<span class="presence">' + "".join(f'<span class="dot {k}"></span>' for k in stores) + '</span>All stores'
    else:
        k = next(k for k, v in STORES.items() if v[0] == store)
        sw = f'<span class="dot {k}"></span>{store}'
    picked = meta.get("picked", 0)
    q = html.escape(meta.get("query", ""))
    search = (f'<label class="search">{icon("search","nav-ico")}<input value="{q}" placeholder="Search samples, runs, files, addresses…" aria-label="Search"><span class="kbd">/</span></label>')
    return f'''<header class="top">
  <a class="brand" href="02-pipelines.html"><span data-seal="bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce" data-size="22"></span>nf-blocks</a>
  <button class="store-switch" type="button" aria-haspopup="listbox">{sw}<span class="chev">▾</span></button>
  {search}
  <div class="right">
    <button class="picked" type="button">Picked <span class="count">{picked}</span></button>
    <span class="me"><span class="av rs">RS</span>rob.syme@seqera.io</span>
  </div>
</header>'''

def nav(meta):
    active = meta.get("nav", "pipelines")
    stores = meta.get("stores", ["a"])
    links = [("projects","Projects","13-project.html", "3" if "b" in stores else ""),
             ("pipelines","Pipelines","02-pipelines.html","2"),
             ("selections","Selections","08-selection.html","1"),
             ("storage","Storage","09-storage.html","")]
    out = ['<nav class="nav" aria-label="Main"><span class="h eyebrow">Browse</span>']
    for key, label, href, n in links:
        on = " on" if key == active else ""
        out.append(f'<a class="{on.strip()}" href="{href}">{icon(key)}<span class="lbl">{label}</span><small>{n}</small></a>')
    ctx = meta.get("navContext", "pipelines")
    if ctx == "pipelines":
        cur = meta.get("navRun")
        out.append('<span class="h eyebrow">nf-core/rnaseq</span>')
        for name, date, st in RUNS:
            on = " on" if name == cur else ""
            col = "var(--fail)" if st == "fail" else "var(--ok)"
            out.append(f'<a class="sub{on}" href="03-run-outputs.html"><span class="dot" style="width:7px;height:7px;background:{col}"></span><span class="lbl">{name}</span><small>{date}</small></a>')
        out.append('<span class="h eyebrow">main.nf</span><a class="sub" href="#"><span class="dot" style="width:7px;height:7px;background:var(--ok)"></span><span class="lbl">sleepy_heyrovsky</span><small>Oct 24</small></a>')
    elif ctx == "projects":
        cur = meta.get("navProject")
        out.append('<span class="h eyebrow">Projects</span>')
        for p in ["RAP1 depletion", "Organoid atlas", "Methods paper"]:
            on = " on" if p == cur else ""
            out.append(f'<a class="sub{on}" href="13-project.html"><span class="lbl">{p}</span></a>')
        out.append('<a class="sub" href="#"><span class="lbl muted">Not in a project</span><small>2</small></a>')
    out.append('<span class="h eyebrow">Stores</span>')
    for k in stores:
        name, size, kind = STORES[k]
        out.append(f'<a href="09-storage.html" title="{kind}"><span class="dot {k}"></span><span class="lbl">{name}</span><small>{size}</small></a>')
    out.append(f'<div class="foot"><span>Snapshot {meta.get("snapshot","2 min ago")} · 0 newer runs</span><span>Index v3 · read by range</span></div></nav>')
    return "\n".join(out)

def spec_panel(meta):
    s = meta.get("spec", {})
    cols = []
    for heading, items in s.items():
        lis = "".join(f"<li>{i}</li>" for i in items)
        cols.append(f"<div><h4>{html.escape(heading)}</h4><ul>{lis}</ul></div>")
    return f'''<details class="spec" open>
  <summary>Spec notes · {html.escape(meta["phase"])} · {html.escape(meta.get("route",""))} — not part of the UI</summary>
  <p style="margin-top:10px;max-width:90ch">{meta.get("summary","")}</p>
  <div class="cols">{"".join(cols)}</div>
</details>'''

def header_comment(meta):
    lines = [f"MOCKUP: {meta['title']}", f"Phase: {meta['phase']}", f"Route: {meta.get('route','')}",
             "", "Purpose:", "  " + re.sub(r"<[^>]+>", "", meta.get("summary","")), "",
             "Data: " + re.sub(r"<[^>]+>", "", meta.get("data","")), "",
             "This file is a visual target. Match layout, hierarchy, copy and states.",
             "The shell, tokens and component classes come from mockups/src/kit.css + kit.js.",
             "Elements outside .app (the .spec panel and .theme-fab) are mockup chrome, not product UI."]
    for heading, items in meta.get("spec", {}).items():
        lines += ["", heading + ":"] + ["  - " + re.sub(r"<[^>]+>", "", i) for i in items]
    return "<!--\n" + "\n".join(l.replace("--", "—") for l in lines) + "\n-->"

def build(path):
    raw = path.read_text()
    m = re.search(r"<!--META\s*(\{.*?\})\s*-->", raw, re.S)
    meta = json.loads(m.group(1))
    body = raw[m.end():]
    main, _, rail = body.partition("<!--RAIL-->")
    main = main.replace("<!--MAIN-->", "").strip()
    layout = meta.get("layout", "app")
    if layout == "app":
        has_rail = bool(rail.strip())
        shell = f'''<div class="app{' has-rail' if has_rail else ''}">
{top(meta)}
{nav(meta)}
<main class="main">
{main}
</main>
{f'<aside class="rail" aria-label="{html.escape(meta.get("railLabel","Use in a workflow"))}">{rail.strip()}</aside>' if has_rail else ''}
</div>
{spec_panel(meta)}'''
    else:
        shell = main + ("\n" + spec_panel(meta) if meta.get("spec") else "")
    doc = f'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>{html.escape(meta.get("fullTitle") or meta["title"] + " · nf-blocks mockup")}</title>
{header_comment(meta)}
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap">
<style>
{CSS}
{meta.get("css","")}
</style>
</head>
<body data-mockup="{meta['file']}">
{shell}
<script>
{JS}
</script>
{"<script>" + meta["js_inline"] + "</script>" if meta.get("js_inline") else ""}
</body>
</html>
'''
    # page-level scripts live in the partial as <script> tags; leave them in place.
    (OUT / meta["file"]).write_text(doc)
    return meta["file"]

if __name__ == "__main__":
    built = [build(p) for p in sorted((SRC / "pages").glob("*.html"))]
    print("\n".join(built))
