// #/identity: who nf-blocks signs as, and how it decided. Also the top bar's "you" chip.
import { h } from '../html.js'
import { avatar, note, pageHead, panel } from '../components.js'
import { SOURCE_TEXT, isAnon } from '../identity.js'

const code = (text) => h('code', {}, text)

/**
 * The top bar's "you": avatar and principal after Picked (mockup 08's .me),
 * a one-line warning when anonymous, nothing when unknown. Links to #/identity.
 */
export function identityChip(identity) {
  if (!identity) return null
  if (isAnon(identity)) {
    return h('a', { class: 'me warn', href: '#/identity', 'data-identity': 'anonymous', title: 'No identity is configured. See Identity.' },
      avatar('anonymous', { size: 'sm', link: false }), h('span', { class: 'who' }, 'No identity: claims will say anonymous'))
  }
  const source = SOURCE_TEXT[identity.source] ?? identity.source
  return h('a', { class: 'me', href: '#/identity', 'data-identity': identity.source, title: `Signing as ${identity.principal}, from ${source}` },
    avatar(identity.principal, { size: 'sm', link: false }), h('span', { class: 'who' }, identity.principal))
}

export function identity(ctx) {
  const id = ctx.identity ?? null
  const said = !id ? null
    : isAnon(id) ? 'No identity is configured, so claims you write will say anonymous.'
      : `from ${SOURCE_TEXT[id.source] ?? id.source}`
  const signing = id
    ? h('div', { class: 'signing' },
      avatar(isAnon(id) ? 'anonymous' : id.principal, { size: 'lg', link: false }),
      h('div', {}, h('div', { class: 'principal' }, isAnon(id) ? 'anonymous' : id.principal), h('div', { class: 'muted' }, said)))
    : h('p', { class: 'muted' }, 'This page isn\'t served by nf-blocks:explore, so it can\'t tell who you are.')
  return h('div', { class: 'identity-page' },
    pageHead({ title: 'Identity' }),
    panel({ title: 'Signing as' }, signing),
    panel({ title: 'How nf-blocks decides' },
      h('p', { class: 'muted' }, 'The first of these that gives a name is used. Only the name is recorded in blocks.'),
      h('ol', {},
        h('li', {}, code('cas.asserted_by'), ' in your Nextflow config. It wins even over a Platform identity.'),
        h('li', {}, 'Your Seqera Platform account, when a token is configured (', code('tower.accessToken'), ' or ', code('TOWER_ACCESS_TOKEN'),
          '). The endpoint is ', code('tower.endpoint'), ' or ', code('TOWER_API_ENDPOINT'), ', default ', code('https://api.cloud.seqera.io'), '.'),
        h('li', {}, 'The ', code('NXF_BLOCKS_USER'), ' environment variable.'),
        h('li', {}, code('git config user.email'), '.'),
        h('li', {}, code('anonymous'), ', with a warning.'))),
    note(h('span', {}, code('cas.asserted_by'), ' in your config overrides every other source. ', code('NXF_BLOCKS_USER=you@example.org'),
      ', set before running nextflow, applies when neither ', code('cas.asserted_by'), ' nor Seqera Platform gives a name: no Platform token is set, or the Platform read failed. Claims you already wrote keep their author.')))
}
