// The query strings of the list routes (#/pipeline/<name>, #/selections, #/projects, #/project/<cid>,
// #/handoff/<template>): where the page sits, whether Mine is on, a project's tab, a hand-off's run and
// samples. They read as the run route does (routequery.js), so a '+' typed into the URL stays a '+'.
import { decoded, queryOf } from './routequery.js'

/** A page offset: a finite number at or above 0, else 0 (`offset=abc` is the first page). */
const offsetOf = (q) => {
  const n = Number(q.get('offset') ?? 0)
  return Number.isFinite(n) && n >= 0 ? n : 0
}

export function parsePipelineQuery(query) {
  const q = queryOf(query)
  return { offset: offsetOf(q), mine: q.get('mine') === '1' }
}

export function parseSelectionsQuery(query) {
  const q = queryOf(query)
  return { offset: offsetOf(q), deleted: q.get('deleted') === '1', mine: q.get('mine') === '1' }
}

/** #/projects' query (P4 Task 2): `unassigned=1` lists the runs and Selections no project holds. */
export function parseProjectsQuery(query) {
  return { unassigned: queryOf(query).get('unassigned') === '1' }
}

/** A project page's tabs (P4 Task 3, mockup 13), in order; the first is the default. */
export const PROJECT_TABS = ['overview', 'samples', 'runs', 'selections', 'activity']

export function parseProjectQuery(query) {
  const tab = queryOf(query).get('tab')
  return { tab: PROJECT_TABS.includes(tab) ? tab : PROJECT_TABS[0] }
}

/**
 * #/handoff/<template>'s query (P4 Task 5): `from` the run it starts from,
 * `where` the Samples tab's filter as text (samples.js parseSamplesWhere reads
 * it), `project` the active project, `samples` the ticked meta.ids,
 * comma-separated (an id with a comma in it is percent-encoded), `outputs`
 * the outputs the hand-off drawer had ticked (per-sample and run-level),
 * comma-separated.
 */
export function parseHandoffQuery(query) {
  const q = queryOf(query)
  // Split before decoding, so an encoded comma stays inside its id.
  const list = (key) => ((query ?? '').split('&').find(p => p.startsWith(`${key}=`))?.slice(key.length + 1) ?? '').split(',').filter(Boolean).map(decoded)
  return { from: q.get('from') || null, where: q.get('where') || null, project: q.get('project') || null,
    samples: list('samples'), outputs: list('outputs') }
}
