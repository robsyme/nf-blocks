// What Save does after the dry run (DESIGN.md §16 decisions 21 and 23). Pure,
// so the outcomes are testable without a DOM.
export function saveChoice(dry) {
  const deletion = dry.deletion ?? 'none'
  if (!dry.exists) return { state: 'new', names: [], prefill: '', supersedes: [], deletion: 'none', restore: [] }
  const names = dry.names ?? []
  if (dry.here ?? true) return { state: 'here', names, prefill: '', supersedes: [], deletion, restore: [] }
  const nameClaims = dry.name_claims ?? []
  const prefill = names.length === 1 && nameClaims.length === 1 ? names[0] : ''
  const restore = deletion === 'none' ? [] : (dry.deletion_claims ?? [])
  return { state: 'elsewhere', names, prefill, supersedes: nameClaims, deletion, restore }
}

// The name Claim to write after a save, or null when no name was typed.
export function namingRequest(choice, typed) {
  const name = (typed ?? '').trim()
  return name ? { name, supersedes: choice.supersedes } : null
}

// The del Claim that restores a copy deleted elsewhere, or null when there is nothing to restore.
export function restoreRequest(choice) {
  return choice.restore.length ? { supersedes: choice.restore } : null
}
