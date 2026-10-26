// web/src/views/kit.js
// #/kit: every kit component with the demo values of
// docs/design/mockups/01-components.html, built by components.js, so the
// explorer can be checked against the mockup side by side. Not linked from the
// nav; it reads nothing from the store.
import { h } from '../html.js'
import { seal } from '../seal.js'
import { identityChip } from './identity.js'
import { formatValue } from '../format.js'
import {
  addrChip, avatar, button, claimTag, durability, empty, factTag, fieldChip, fileChip, kindChip, kv, minibar, note,
  pageHead, panel, presence, status, storageBar, tabs, thread,
} from '../components.js'

const RAW = 'bafkreibxyh4xa4v5od634co6m4wcvs3yrh7s4vhsz2hgiybzwkahgz3oym'
const RAW_2 = 'bafkreibr5nkdlnukzyfhxj2rcgtoxsfztlli2fr6lfmyxekn2iow22ol2u'
const RUN = 'bafyreibimvfollxtcc5szqp5qfmwqi6vreknpathclrcliwnvzir26ocxq'
const SELECTION = 'bafyreib2rljbxplvhdgbxoqfxujbc2bhztqueg7u35xuuurdrpn2id5hce'
const MANIFEST = 'bafyreifsax6ih4vi5vcnbvti24wtjwneszrifujtyugwmikgl7n4sz6jnq'
const TOKENS = ['--ink', '--ink-2', '--ink-3', '--bg', '--surface', '--sunken', '--line', '--accent', '--accent-ink', '--accent-soft',
  '--claim-bg', '--claim-line', '--store-a', '--store-b', '--store-c', '--pinned', '--held', '--releasing', '--missing', '--ok', '--fail']
const HOUR = 3_600_000

const row = (title, code, text, ...demo) => h('div', { class: 'kit-row' },
  h('div', { class: 'kit-lbl' }, h('h2', {}, title), h('code', {}, code), h('p', {}, text)),
  h('div', { class: 'kit-demo' }, demo))

const muted = (text) => h('span', { class: 'muted', style: 'font-size:12.5px' }, text)

function filterChip(label, op, value, src = null) {
  return h('span', { class: 'fchip' }, h('span', {}, label), src ? h('span', { class: 'src' }, src) : null, h('span', { class: 'op' }, op),
    h('input', { value, 'aria-label': 'value' }), h('button', { class: 'x', type: 'button', 'aria-label': 'Remove' }, '×'))
}

export function kit() {
  const now = Date.now()
  return h('div', { class: 'kit-sheet' },
    h('div', { class: 'kit-intro' }, h('span', { class: 'eyebrow' }, 'nf-blocks explorer · kit'), h('h1', {}, 'Components'),
      h('p', { class: 'muted' }, 'Each component as components.js builds it, with the demo values of the mockup sheet (mockups/01-components.html).')),
    row('Seal', 'seal(cid, { size })', 'Deterministic glyph from the CID. Squares are raw file blocks (bafk…); circles are records (bafy…). Sizes: 14, 16, 18, 20, 30, 36.',
      [RAW, RAW_2, RUN, SELECTION].map(c => seal(c, { size: 36 })), seal(SELECTION, { size: 20 }), seal(SELECTION, { size: 14 })),
    row('Address chip', 'addrChip(text, { ref, seal })', 'Click copies the full reference in data-ref (cas://… or lid://…). Never shows a filesystem path.',
      addrChip(RAW, { ref: `cas://${RAW}/salmon.merged.gene_counts.tsv` }), addrChip('lid://e25b50f2755ea17dffe98a907420b06f', { seal: false })),
    row('Fact tag', 'factTag(kind, label, cid, href)', 'An immutable record: kind label and name, with its Seal.',
      factTag('run', 'distraught_murdock', RUN), factTag('selection', '3 items', SELECTION), factTag('manifest', 'nf-core/rnaseq', MANIFEST)),
    row('Claim tag', 'claimTag({ assertedBy, verb, value, at, conflict })', 'A signed statement: avatar, verb, value, age. .conflict when current claims disagree.',
      claimTag({ assertedBy: 'ada.lovelace@example.org', verb: 'named', value: 'wigshop', at: now - 3 * HOUR, tone: 'rs', now }),
      claimTag({ assertedBy: 'ana.kim', verb: 'pinned', value: 'paper fig 2', at: now - 50 * HOUR, tone: 'ak', now }),
      claimTag({ assertedBy: 'ada.lovelace@example.org', verb: 'released', value: h('span', {}, 'sharp_magritte'), tone: 'rs' }),
      claimTag({ assertedBy: 'jo.tan', verb: 'named', value: 'wig-final', conflict: true, tone: 'jt' }),
      claimTag({ assertedBy: 'anonymous', verb: 'named', value: h('span', {}, 'by anonymous') })),
    row('Avatar', "avatar(assertedBy, { size, tone })", 'Initials of asserted_by. Anonymous uses a dashed outline.',
      avatar('ada.lovelace@example.org', { size: 'lg', tone: 'rs' }), avatar('ana.kim', { size: 'md', tone: 'ak' }), avatar('jo.tan', { tone: 'jt' }), avatar('anonymous', { size: 'md' })),
    row('You chip', 'identityChip(identity)', 'The top bar: who is signing and from where (title). Anonymous warns and links to Identity; the anonymous avatar above links there too.',
      identityChip({ principal: 'ada.lovelace@example.org', source: 'git' }), identityChip({ principal: 'anonymous', source: 'anonymous' })),
    row('Presence', 'presence([{ key, name, held }])', 'Filled: held in that store. Ring: referenced, not held. The tooltip names the stores.',
      presence([{ key: 'a', name: 'roblab', held: true }, { key: 'b', name: 'org-s3', held: true }, { key: 'c', name: 'ana-hpc', held: false }]),
      muted('roblab, org-s3 · referenced in ana-hpc'), presence([{ key: 'a', name: 'roblab', held: false }]), muted('input, by path')),
    row('Durability', "durability(state, text)", 'Will it survive the next sweep in this store? Releasing always includes a date.',
      durability('pinned', 'Pinned'), durability('held', 'Held by run'), durability('releasing', 'Releasing · Oct 31'), durability('missing', 'Missing here')),
    row('Run status', 'status(run)', 'Icon and word, never colour alone.',
      status({ status: 'succeeded' }), status({ status: 'failed' }), status({ status: 'succeeded', possibly_incomplete: 1 }), status({ status: 'running' })),
    row('Storage bar', 'storageBar({ total, pinned, held, releasing, yours })', 'One store only. Stacked pinned / held / releasing, then a separate thin "Yours" bar. Sizes in bytes.',
      h('div', { class: 'kit-stack' },
        storageBar({ total: 1820000000000, pinned: 640000000000, held: 1020000000000, releasing: 160000000000, yours: 212000000000, store: 'b', storeName: 'org-s3' }),
        storageBar({ total: 142134100, pinned: 0, held: 142134100, releasing: 0, yours: 142134100, store: 'a', storeName: 'roblab' }))),
    row('File chips', 'fileChip · kindChip · fieldChip', 'Files grouped by tool with extensions and counts. Kind chips summarise outputs. Field chips show formatted values.',
      fileChip('fastqc', [['zip', 6], ['html', 6]], { href: '#/kit' }), fileChip('trim', [['txt', 2]], { href: '#/kit' }), kindChip('bam', 5), kindChip('bigWig', 15),
      fieldChip('Reads (trimmed)', formatValue('float', '99566.0')), fieldChip('% mapped', formatValue('float', '89.0', { format: 'percent' }))),
    row('Filter chip', '.fchip > .src .op input .x', 'Shows the source output for cross-output filters.',
      filterChip('Reads (trimmed)', '≥', '50000', 'preprocessed'), filterChip('Layout', '=', 'paired')),
    row('Lineage thread', 'thread([{ text, href, here, ext }])', 'Inputs at top, current node highlighted, consumers below. Dashed node: not stored.',
      h('div', { class: 'kit-block' }, thread([{ text: [h('span', {}, 'WT_REP1_1.fastq.gz'), ' ', h('span', { class: 'muted' }, '· input, by path')], ext: true },
        { text: 'preprocessed · WT_REP1' }, { text: 'quant_salmon · WT_REP1', here: true }, { text: 'quant_merged · all_samples' }]))),
    row('Buttons', "button(label, { kind, size })", 'Primary: the one forward action. Danger: release or delete. Labels state the outcome.',
      button('Copy 13 blocks', { kind: 'primary' }), button('Pin…'), button('Release (frees 63.7 MB)', { kind: 'danger' }), button('+ Add filter', { kind: 'ghost' }),
      button('Pick', { size: 'sm' }), button('Pin 5.29 MB', { disabled: true })),
    row('Notes and empty states', "note(text, tone) · empty(title, text)", 'Explain, then say what to do.',
      h('div', { class: 'kit-stack' }, note('Provenance is complete at the destination.', 'ok'), note('Each condition needs at least 2 replicates.', 'warn'),
        note("Block bafk…gz3oym isn't in roblab. It's held in org-s3.", 'bad'),
        empty('Nothing pinned yet', 'Pinned items stay in this store after their runs are released.'))),
    row('Page parts', 'pageHead · tabs · panel · kv · minibar', 'The head of every page, its tabs, and a panel holding a definition list and a bar.',
      h('div', { class: 'kit-stack' },
        pageHead({ crumbs: [{ text: 'Pipelines', href: '#/kit' }, { text: 'nf-core/rnaseq', href: '#/kit' }, { text: 'distraught_murdock' }], seal: RUN,
          title: 'distraught_murdock', after: [status({ status: 'succeeded' })], actions: [button('Pin…')],
          subline: [h('span', {}, h('b', {}, '17 outputs')), 'Oct 24, 1:29 PM', addrChip(RUN)] }),
        tabs([{ key: 'outputs', label: 'Outputs', href: '#/kit', count: 17, on: true }, { key: 'storage', label: 'Storage', href: '#/kit' }]),
        panel({ title: 'Stored', meta: 'in roblab' }, kv([['Files', '412'], ['Stored', '142 MB'], ['Share', h('span', { class: 'row' }, minibar(0.64), '64%')]])))),
    row('Tokens', ':root', 'Seqera base, the store set and the durability set. Both themes.',
      h('div', { class: 'kit-tokens' }, TOKENS.map(t => h('div', { class: 'kit-token' }, h('div', { style: `background:var(${t})` }), h('div', { class: 'mono' }, t))))))
}
