// What Save does after the dry run (DESIGN.md §16 decision 21). Pure, so the
// three outcomes are testable without a DOM.
export function saveChoice(dry) {
  if (!dry.exists) return { state: 'new', names: [], prefill: '', supersedes: [] }
  const names = dry.names ?? []
  if (dry.here ?? true) return { state: 'here', names, prefill: '', supersedes: [] }
  const nameClaims = dry.name_claims ?? []
  const prefill = names.length === 1 && nameClaims.length === 1 ? names[0] : ''
  return { state: 'elsewhere', names, prefill, supersedes: nameClaims }
}

// The name Claim to write after a save, or null when no name was typed.
export function namingRequest(choice, typed) {
  const name = (typed ?? '').trim()
  return name ? { name, supersedes: choice.supersedes } : null
}
