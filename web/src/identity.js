// Who is signing, as the server reported it in members.json (`identity: { principal, source }`).

export const SOURCE_TEXT = {
  config: 'cas.asserted_by in your config',
  platform: 'your Seqera Platform account',
  env: 'NXF_BLOCKS_USER',
  git: 'git config user.email',
  anonymous: 'nothing configured',
}

/** The comparison form of a principal, the same rule as SQL's lower(trim(x, char(32,9,10,13,12,11))): ASCII whitespace off both ends, ASCII letters lowercased. */
export const principalKey = (s) => String(s ?? '').replace(/^[ \t\n\r\f\v]+|[ \t\n\r\f\v]+$/g, '').replace(/[A-Z]/g, ch => ch.toLowerCase())

/** The same person: ASCII whitespace trimmed, ASCII case ignored. Empty never matches. */
export const samePrincipal = (a, b) => principalKey(a) !== '' && principalKey(a) === principalKey(b)

/**
 * The one anonymous rule for the page: no identity, a blank name, the
 * anonymous source, or a name that reads `anonymous` in any case. The avatar
 * applies it to a claim's `asserted_by` as `isAnon({ principal: who })`.
 */
export const isAnon = (identity) => !identity || principalKey(identity.principal) === '' || identity.source === 'anonymous' || principalKey(identity.principal) === 'anonymous'

/** The viewer's principal for `ctx.viewer`, or null when unknown or anonymous. */
export const viewerOf = (identity) => (isAnon(identity) ? null : identity.principal)

/** A principal as the snapshot's per-person storage tables key it (StorageStats.asserter): principalKey, and 'anonymous' for none. */
export const asserterKey = (s) => principalKey(s) || 'anonymous'

/**
 * A principal in words: "you" for the viewer, "anonymous" for none, else as
 * the Claim or snapshot gave it. `capital` starts "You" and "Anonymous" upper
 * case, for a table cell or the start of a list entry.
 */
export function whoText(who, viewer, { capital = false } = {}) {
  const text = samePrincipal(who, viewer) ? 'you' : isAnon({ principal: who }) ? 'anonymous' : who
  return capital && (text === 'you' || text === 'anonymous') ? text[0].toUpperCase() + text.slice(1) : text
}

/** Whether the Mine filter can work, and why not. */
export function mineState(identity) {
  if (!identity) return { available: false, reason: 'Mine needs this page opened through nextflow plugin nf-blocks:explore, which knows who you are.' }
  if (isAnon(identity)) return { available: false, reason: 'No identity is configured, so none of these can be marked as yours.' }
  return { available: true, reason: null }
}
