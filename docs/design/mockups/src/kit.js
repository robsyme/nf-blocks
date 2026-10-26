/* ==========================================================================
   nf-blocks mockup kit — behaviour shared by every page.
   Agents: the Seal algorithm below is normative. Port it exactly so the same
   CID draws the same glyph in the explorer, in exports and in these mockups.
   ========================================================================== */
(function(){
  'use strict';

  /* ---- Seal ---------------------------------------------------------------
     Input: CID string (base32, 'b' multibase prefix).
     1. kind = raw file block if CID starts with "bafk", else record (dag-cbor).
     2. seed = FNV-1a 32-bit over CID.slice(4) (skip the shared "bafk"/"bafy").
     3. PRNG = mulberry32(seed).
     4. 5×5 grid, mirrored left↔right: for row 0..4, col 0..2 draw a cell if
        rng() < 0.5 (col 2 is the centre column, drawn once).
     5. Cells: raw → rounded squares; record → circles.
     6. Tick: one more rng() picks a corner (0..3) for a small accent dot,
        so mirror-image twins still differ.
     Frame: rounded square, surface fill, --line-2 stroke. Cells in --ink. */
  function fnv1a(str){let h=2166136261>>>0;for(let i=0;i<str.length;i++){h^=str.charCodeAt(i);h=Math.imul(h,16777619)>>>0}return h}
  function mulberry32(a){return function(){a|=0;a=a+0x6D2B79F5|0;let t=Math.imul(a^a>>>15,1|a);t=t+Math.imul(t^t>>>7,61|t)^t;return((t^t>>>14)>>>0)/4294967296}}
  function seal(cid,size){
    size=size||20; const raw=/^bafk/.test(cid); const r=mulberry32(fnv1a(cid.slice(4)));
    const n=5,pad=size*0.14,cell=(size-pad*2)/n; let cells='';
    for(let y=0;y<n;y++)for(let x=0;x<3;x++){ if(r()<0.5){ const cols=x===2?[2]:[x,n-1-x];
      for(const cx of cols){const px=pad+cx*cell,py=pad+y*cell;
        cells+=raw?`<rect x="${(px+cell*.08).toFixed(2)}" y="${(py+cell*.08).toFixed(2)}" width="${(cell*.84).toFixed(2)}" height="${(cell*.84).toFixed(2)}" rx="${(cell*.14).toFixed(2)}"/>`
                  :`<circle cx="${(px+cell/2).toFixed(2)}" cy="${(py+cell/2).toFixed(2)}" r="${(cell*.42).toFixed(2)}"/>`;}}}
    const tick=Math.floor(r()*4),tx=[pad*.5,size-pad*.5][tick&1],ty=[pad*.5,size-pad*.5][tick>>1];
    return `<svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" role="img" aria-label="seal ${cid.slice(0,4)}…${cid.slice(-6)}"><rect x=".5" y=".5" width="${size-1}" height="${size-1}" rx="${(size*.2).toFixed(2)}" style="fill:var(--surface);stroke:var(--line-2)"/><g style="fill:var(--ink)">${cells}</g><circle cx="${tx}" cy="${ty}" r="${Math.max(1.2,size*.035).toFixed(2)}" style="fill:var(--accent)"/></svg>`;
  }
  function shortCid(c){return c.slice(0,4)+'…'+c.slice(-6)}

  /* ---- Bytes: binary-free SI, 3 significant figures ---- */
  function bytes(n){if(n==null)return '—';const u=['B','KB','MB','GB','TB','PB'];let i=0;while(n>=1000&&i<u.length-1){n/=1000;i++}
    return (i===0?n:n>=100?n.toFixed(0):n>=10?n.toFixed(1):n.toFixed(2))+' '+u[i]}

  /* ---- Storage bar: data-bar='{"total":B,"pinned":B,"held":B,"releasing":B,"yours":B?,"store":"a|b|c"?,"storeName":"…"?}' (bytes) */
  function renderBar(el){const d=JSON.parse(el.dataset.bar),T=d.total||1,p=v=>(v/T*100).toFixed(2)+'%',pct=v=>Math.round(v/T*100);
    el.classList.add('sbar');
    el.innerHTML=(d.store?`<div class="storehd"><span class="dot ${d.store}"></span>${d.storeName} <span class="muted" style="font-weight:400">· ${bytes(d.total)}</span></div>`:'')+
      `<div class="track" role="img" aria-label="${bytes(d.pinned)} pinned, ${bytes(d.held)} held, ${bytes(d.releasing)} releasing, of ${bytes(T)}"><div class="seg pinned" style="width:${p(d.pinned)}"></div><div class="seg held" style="width:${p(d.held)}"></div><div class="seg releasing" style="width:${p(d.releasing)}"></div></div>`+
      `<div class="legend"><span><i style="background:var(--pinned)"></i>Pinned ${bytes(d.pinned)}</span><span><i style="background:var(--held);opacity:.55"></i>Held ${bytes(d.held)}</span><span><i style="background:repeating-linear-gradient(135deg,var(--releasing) 0 3px,transparent 3px 5px)"></i>Releasing ${bytes(d.releasing)}</span></div>`+
      (d.yours!=null?`<div class="mine"><b>Yours</b><span class="mt" role="img" aria-label="yours ${bytes(d.yours)}"><span style="width:${p(d.yours)}"></span></span><span class="num">${bytes(d.yours)} · ${pct(d.yours)}%</span></div>`:'');}

  function hydrate(root){
    (root||document).querySelectorAll('[data-seal]').forEach(el=>{if(el.dataset.done)return;el.classList.add('seal');el.innerHTML=seal(el.dataset.seal,+el.dataset.size||20);el.dataset.done=1});
    (root||document).querySelectorAll('[data-bar]').forEach(renderBar);
    (root||document).querySelectorAll('[data-bytes]').forEach(el=>{el.textContent=bytes(+el.dataset.bytes)});
  }

  /* ---- Copy full cas:// reference from address chips ---- */
  document.addEventListener('click',e=>{const a=e.target.closest('.addr[data-ref]');if(!a)return;
    const t=a.dataset.ref; (navigator.clipboard?navigator.clipboard.writeText(t):Promise.reject()).then(()=>flash(a,'Copied'),()=>flash(a,'Copy failed'));});
  function flash(el,msg){const o=el.getAttribute('title');el.setAttribute('title',msg);el.style.borderColor='var(--accent)';setTimeout(()=>{el.style.borderColor='';o?el.setAttribute('title',o):el.removeAttribute('title')},900)}

  /* ---- Mockup chrome: theme toggle + spec pins (not product UI) ---- */
  function fab(){const f=document.createElement('div');f.className='theme-fab';
    f.innerHTML='<button type="button" data-t="light">Light</button><button type="button" data-t="dark">Dark</button><button type="button" data-t="pins">Spec pins</button>';
    f.addEventListener('click',e=>{const b=e.target.closest('button');if(!b)return;
      if(b.dataset.t==='pins'){document.body.classList.toggle('show-pins');return}
      document.documentElement.setAttribute('data-theme',b.dataset.t)});
    document.body.appendChild(f)}

  window.NFB={seal,shortCid,bytes,hydrate,renderBar};
  document.addEventListener('DOMContentLoaded',()=>{hydrate();fab()});
})();
