// The agreed Lineage panel (spec 2026-10-28-lineage-provenance-tree), drawn
// with the explorer's own components over real roblab CIDs. Not shipped: the
// build script bundles it into mockup.html, and shoot.mjs screenshots that.
// The tree's CSS here is the mockup's; the implementation owns its own.
import { h } from '../../../../web/src/html.js'
import { KIT_CSS } from '../../../../web/src/kit-css.js'
import { APP_CSS } from '../../../../web/src/styles.js'
import { ICON_CSS, ref } from '../../../../web/src/icons.js'
import { panel, runFact } from '../../../../web/src/components.js'
import { seal } from '../../../../web/src/seal.js'

const RUN = {
  fab: { name: 'fabulous_bartik', cid: 'bafyreibs42aetspf36hkfd5gi7huxke7ecacdkwssiuxgb7pbnqyhetcje', when: '2 days ago', people: 'bafyreie5vbrq6cmxklta6n5kerdn7jjg2itvxix6plbwka2xxu67jq3cv4' },
  sleepy: { name: 'sleepy_williams', cid: 'bafyreihgmzdqro7klxhvbnlknofq4o37f5xa4fcg7ocjw4l7wf35zjn2ne', when: '1 day ago', people: 'bafyreib4v3fh4cpl72tmhze2ry34442efyqir75dg4h476ifxldenjglva' },
  // Made up, to show "also 3 other runs".
  keen: { name: 'keen_hopper', cid: 'bafyreihq2kd7wz4m5xprt3nqyfa6c2ubvle7goixzsh4tk3jdnwqpmy5ra' },
  brave: { name: 'brave_lovelace', cid: 'bafyreicx3n7ylp2vqhrm4wzt6ukd5bfa2ojesq7gilcnhxbyk4atru3vme' },
}
const FRIENDS = 'bafyreigcbmmvs6eetctevnluoq4pimk3g5zplrpuf6lswumfltvsqryaoq'
const COMMUNITY = 'bafyreibhevzzjhhwcagkpl2hxq4ppbmhdynagdqxvxxs4xm6t7lzn4i54e'
// [title, item, file, address, size, runs whose Origins name the item]
const P = {
  Alice: ['Alice', 'bafyreigws6u7hfkhysjocq4gsmujlher57kz3uog6kq34vvd4g7lrwjbfa', 'Alice_Smith.dat', 'bafkreiapov4akcs3ubp64gnjd3bgnxn7wvuetb7cubplrrsakek5c62sbu', 32],
  Amina: ['Amina', 'bafyreicdxmnitqv2hdse46dxcfulzptkw3l7g3qgoe7wtdj4n6ysgnbynq', 'Amina_Okafor.dat', 'bafkreibvufxgttdb3bcj7kv5clp6g3e2i6xagiej3ksxsbceili4zhyav4', 34],
  Bob: ['Bob', 'bafyreighkiwzczvnexf2xqgeepcyhstpuom7aiocrar6hhhn4ife5c4eny', 'Bob_Jones.dat', 'bafkreiehbe7qfto54avyummmbb54lintaalbywvt7edh7anr34smkjzoo4', 31],
  Carmen: ['Carmen', 'bafyreigvlzydfmxcoti4bucxiwhmkirjd4sc6pbblbkehz4pqj4jmjtvsq', 'Carmen_Ruiz.dat', 'bafkreievequkbgnkhxcguez7hadmsa3twpbavejnhiubftrdjghrfwtlxq', 32],
  Dev: ['Dev', 'bafyreiejwemck5f3kp4svrbh4zwfawcbzqqdwov2eevea7k5qhyxy3j3s4', 'Dev_Patel.dat', 'bafkreiabwrgnn5kcvicddr535e76trlho45kpjhsboh3b5kqnlryzora4y', 28],
  Emi: ['Emi', 'bafyreietdw3pzxbocjqm7ewqz3f4632kmf2diiv3wccgjiknygso3kcga4', 'Emi_Tanaka.dat', 'bafkreidhkk4e3k74yjb55mveub64c3wd52y6usi3qzz4wgqcfv3tenfd64', 28],
  Femi: ['Femi', 'bafyreibkrh6s2k7vkbjqikcqlcwnizyf7cuau5ttk5s4prjdpeufyj3cti', 'Femi_Adeyemi.dat', 'bafkreiai66dqrfgp5nstnjbnfy2qzlhnxivcnc5nfqv42vn27xd3ydfba4', 32],
  Greta: ['Greta', 'bafyreib523rlbrpvp5yj7z26i5sy4ytiz4ir5ef67q6hioicpleic4ljsi', 'Greta_Larsen.dat', 'bafkreigmxh3yragnbxpqg57qhjfp5rflh2adrpyzr5cwb4zj4526xpxqxi', 31],
  Tarik: ['Tarik', 'bafyreian3sfv6mv6jh2en6g3ryqo2p2lde4nh3zxaysbhs5g574uac6uye', 'Tarik_Benali.dat', 'bafkreifhmpsnesszvz47yrivj7jyuy36ndf3sz3rlycz5nj55qeuxkevdy', 37],
}

const ok = (name) => ({ run_name: name, status: 'succeeded', possibly_incomplete: 0 })
const muted = (...t) => h('span', { class: 'muted small' }, ...t)
const fileRef = (name, cid) => h('span', { class: 'mono', style: 'font-size:12px' }, ref('file', name, '#', { cid }))
const miniRun = (k) => h('a', { class: 'also', href: '#' }, seal(RUN[k].cid, { size: 12 }), RUN[k].name)
// "also" sits outside the box in both forms: one other run is named, two or more are counted (names in the title).
const also = (keys) => keys.length === 0 ? null
  : [muted('also '), keys.length === 1 ? miniRun(keys[0])
    : h('span', { class: 'also', title: keys.map(k => RUN[k].name).join(', ') }, `${keys.length} other runs`)]

const CSS = `
body{padding:24px 16px}
.wrap{max-width:672px;margin:0 auto;display:grid;gap:26px}
.wrap > h2{font-size:18px;font-weight:650}
.cap{color:var(--ink-2);font-size:var(--fs-sm);margin-top:-18px}
.lbl{font-family:var(--font-mono);font-size:10.5px;text-transform:uppercase;letter-spacing:.08em;color:var(--ink-3);margin-bottom:8px}
.col{display:grid;gap:14px}
.ghost{border:1px solid var(--line);border-radius:var(--r-lg);background:var(--surface);padding:12px 16px;color:var(--ink-3);font-size:var(--fs-sm)}
.ptree{position:relative;padding-left:22px}
.ptree::before{content:"";position:absolute;left:8px;top:16px;bottom:16px;width:2px;background:var(--line-2)}
.pg{position:relative;padding:4px 0 8px}
.pg + .pg{margin-top:4px}
.pg .gh{position:relative;display:flex;gap:8px;align-items:center;flex-wrap:wrap;min-height:32px}
.pg .gh::before,.ptree .here::before{content:"";position:absolute;left:-18px;top:16px;width:10px;height:10px;margin-top:-5px;border-radius:50%;background:var(--surface);border:2px solid var(--ink-3)}
.pg .sub{display:flex;gap:6px 8px;align-items:center;flex-wrap:wrap;margin:4px 0 0 12px;font-size:var(--fs-sm)}
.pitems{margin:6px 0 0 12px;padding-left:16px;border-left:2px solid var(--line)}
.pitem{position:relative;display:grid;grid-template-columns:minmax(90px,max-content) minmax(140px,max-content) 1fr auto;gap:14px;align-items:center;padding:4px 0;font-size:var(--fs-sm)}
.pitem::before{content:"";position:absolute;left:-16px;top:14px;width:10px;border-top:2px solid var(--line)}
.pitem .sz{color:var(--ink-3);font-variant-numeric:tabular-nums;font-size:12px;text-align:right}
.pitem .tag{display:flex;gap:5px;align-items:center}
.also{display:inline-flex;gap:5px;align-items:center;font-size:11.5px;color:var(--ink-3);border:1px solid var(--line);border-radius:var(--r-sm);padding:1px 6px 1px 3px;white-space:nowrap}
span.also{padding:1px 6px;cursor:help}
.also .seal svg{display:block}
.ptree .here{position:relative;display:flex;gap:8px;align-items:center;flex-wrap:wrap;padding:6px 0;font-size:var(--fs-sm)}
.ptree .here::before{background:var(--accent);border-color:var(--accent-ink)}
`

const row = ([title, item, file, address, size], others) =>
  h('div', { class: 'pitem' }, ref('item', title, '#', { cid: item }), fileRef(file, address), h('span', { class: 'tag' }, also(others)), h('span', { class: 'sz' }, `${size} B`))

/** One upstream run's group: run chip, then the output and Selection line, then a row per input. */
const group = (k, { selection, name, total, rows }) => h('div', { class: 'pg' },
  h('div', { class: 'gh' }, runFact(ok(RUN[k].name), RUN[k].cid), muted(`scratch-workflow · ${RUN[k].when}`)),
  h('div', { class: 'sub' }, ref('output', 'people', '#', { cid: RUN[k].people }), muted(`${rows.length} of ${total} items, via`), ref('selection', `"${name}"`, '#', { cid: selection })),
  h('div', { class: 'pitems' }, rows.map(([p, others]) => row(P[p], others))))

const here = (title, item, run, runCid) => h('div', { class: 'here' }, ref('item', h('b', {}, title), null, { cid: item }), muted('made by'), runFact(ok(run), runCid))
const producedBy = (run, runCid) => panel({ title: 'Produced by' }, h('div', { class: 'row', style: 'gap:8px' }, runFact(ok(run), runCid), muted('10m')))
const page = (lineage, run, runCid) => h('div', { class: 'col' }, h('div', { class: 'ghost' }, 'Files panel (unchanged)'), lineage,
  h('div', { class: 'grid-2' }, producedBy(run, runCid), h('div', {})))

const one = panel({ title: 'Lineage', meta: '1 upstream run · 4 inputs' }, h('div', { class: 'ptree' },
  group('fab', { selection: FRIENDS, name: 'Friends', total: 9, rows: [['Amina', []], ['Dev', []], ['Femi', []], ['Greta', []]] }),
  here('circle · 4', 'bafyreiez7zewl7goyaaclrefrzs3rbwfycz4rmxydrpfmzijaujdihr76i', 'goofy_dubinsky', 'bafyreigrctdne4mfcwerx4aphy43ikn37phcyjrxln7vcqt46ca2vvpltm')))

const shared = ['sleepy', 'keen', 'brave']
const two = panel({ title: 'Lineage', meta: '2 upstream runs · 9 inputs' }, h('div', { class: 'ptree' },
  group('fab', { selection: COMMUNITY, name: 'Community', total: 9, rows: [['Alice', []], ['Amina', shared], ['Bob', []], ['Carmen', []], ['Dev', ['sleepy']],
    ['Emi', []], ['Femi', ['sleepy']], ['Greta', []], ['Tarik', []]] }),
  group('sleepy', { selection: COMMUNITY, name: 'Community', total: 9, rows: [['Amina', ['fab', 'keen', 'brave']], ['Dev', ['fab']], ['Femi', ['fab']]] }),
  here('circle · 9', 'bafyreidgx4yv434qpepw3z6qzpzihsupde25g5xwbaz3wevewhkizy4oum', 'magical_ritchie', 'bafyreidoorighziz7cyap7flky4d2ro3bzlrtuvrtg5yfxstnbonzettvi')))

document.head.append(h('style', {}, KIT_CSS), h('style', {}, APP_CSS), h('style', {}, ICON_CSS), h('style', {}, CSS))
document.body.append(h('div', { class: 'wrap' },
  h('h2', {}, 'Lineage panel: one group per upstream run'),
  h('p', { class: 'cap' }, 'The main column at 672px, its width in a 1280px window. Real roblab CIDs, except keen_hopper and brave_lovelace, made up to show the collapsed "also" tag on Amina.'),
  h('div', {}, h('div', { class: 'lbl' }, 'One upstream run: circle · 4 from goofy_dubinsky'), page(one, 'goofy_dubinsky', 'bafyreigrctdne4mfcwerx4aphy43ikn37phcyjrxln7vcqt46ca2vvpltm')),
  h('div', {}, h('div', { class: 'lbl' }, 'Two upstream runs through one Selection: circle · 9 from magical_ritchie'), page(two, 'magical_ritchie', 'bafyreidoorighziz7cyap7flky4d2ro3bzlrtuvrtg5yfxstnbonzettvi'))))
