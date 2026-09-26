// The compose save sequence (DESIGN.md §16 decisions 21, 23 and 24): the
// Selection, then the name Claim if a name is set, then the del if restoring.
// Once the Selection is saved nothing throws: failures come back to the caller,
// and the del is tried even when naming failed.
import { namingRequest, restoreRequest } from './save-choice.js'

const failure = (step, e, extra = {}) => ({ step, code: e?.code ?? 'write_failed', message: e?.message ?? String(e), ...extra })

export async function saveSequence(writer, members, choice, typedName, { onSaved } = {}) {
  const written = await writer.selection(members)
  onSaved?.()
  const failures = []
  const naming = namingRequest(choice, typedName)
  if (naming) {
    try {
      await writer.rename(written.address, naming.name, naming.supersedes)
    } catch (e) {
      failures.push(failure('naming', e))
    }
  }
  const restoring = restoreRequest(choice)
  if (restoring) {
    try {
      await writer.undo(written.address, restoring.supersedes)
    } catch (e) {
      failures.push(failure('restoring', e, { retry: restoring.supersedes }))
    }
  }
  return { address: written.address, failures }
}

export async function retryRestore(writer, address, supersedes) {
  await writer.undo(address, supersedes)
}

// The failure banner a partly failed save carries to the saved Selection's
// page, possibly in another member's page (one sessionStorage value).
export const bannerText = (banner) => JSON.stringify(banner)

/** The carried banner, or null when there is none or it cannot be read. */
export function bannerFrom(text) {
  if (!text) return null
  try {
    const b = JSON.parse(text)
    return typeof b?.address === 'string' && Array.isArray(b.failures) && b.failures.length ? b : null
  } catch {
    return null
  }
}
