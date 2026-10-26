// web/src/routes.js
// The location hash's routes (DESIGN.md §15). Each has a name, a pattern,
// `args(m)`, which reads the match and its query into what the view takes,
// and `view(ex, args, ctx)`, which draws it. app.js finds the route and calls
// both; tests call `args` the same way. Nothing runs at import.
import * as views from './views/index.js'
import { kit } from './views/kit.js'
import { parseCopyQuery } from './copy.js'
import { highlightOf } from './filepreview.js'
import { queryOf } from './routequery.js'
import { parseHandoffQuery, parsePipelineQuery, parseProjectQuery, parseProjectsQuery, parseSelectionsQuery } from './listquery.js'

const route = (name, pattern, args, view) => ({ name, pattern, args, view })
const none = () => ({})

export const ROUTES = [
  // Copy to another member (P3 Task 4): before the run and Selection routes, whose patterns stop at the cid.
  route('copy', /^#\/(selection|run)\/([^/?]+)\/copy(?:\?(.*))?$/, m => ({ kind: m[1], cid: m[2], query: parseCopyQuery(m[3]) }),
    (ex, a, ctx) => views.copyView(ex, a.kind, a.cid, ctx, a.query)),
  route('home', /^#?\/?$/, none, (ex, a, ctx) => views.home(ex, ctx)),
  route('idle', /^#\/idle$/, none, () => views.idle()),
  route('pipeline', /^#\/pipeline\/([^/?]+)(?:\?(.*))?$/, m => ({ pipeline: decodeURIComponent(m[1]), ...parsePipelineQuery(m[2]) }),
    (ex, a, ctx) => views.pipeline(ex, a.pipeline, a.offset, ctx, { mine: a.mine })),
  route('run', /^#\/run\/([^/?]+)(?:\?(.*))?$/, (m) => {
    const q = queryOf(m[2])
    return { cid: m[1], tab: q.get('tab') ?? null, where: q.get('where') ?? null, show: q.get('show') ?? null, release: q.get('release') === '1' }
  }, (ex, { cid, ...query }, ctx) => views.run(ex, cid, ctx, query)),
  route('collection', /^#\/collection\/([^/?]+)(?:\?offset=(\d+))?$/, m => ({ cid: m[1], offset: Number(m[2] ?? 0) }),
    (ex, a, ctx) => views.collection(ex, a.cid, a.offset, ctx)),
  route('item', /^#\/item\/([^/]+)\/([^/]+)$/, m => ({ collection: m[1], item: m[2] }), (ex, a, ctx) => views.item(ex, a.collection, a.item, ctx)),
  route('content', /^#\/content\/([^/?]+)(?:\?(.*))?$/, m => ({ cid: m[1], highlight: highlightOf(queryOf(m[2]).get('highlight')) }),
    (ex, a, ctx) => views.content(ex, a.cid, ctx, { highlight: a.highlight })),
  route('block', /^#\/block\/([^/?]+)$/, m => ({ cid: m[1] }), (ex, a, ctx) => views.block(ex, a.cid, ctx)),
  route('latest', /^#\/latest\/([^/]+)$/, m => ({ pipeline: decodeURIComponent(m[1]) }), (ex, a) => views.latest(ex, a.pipeline)),
  route('items', /^#\/items\/([^/]+)\/([^/?]+)(?:\?where=(.*))?$/, m => ({ collection: m[1], path: decodeURIComponent(m[2]),
    where: m[3] ? decodeURIComponent(m[3]) : '[]' }), (ex, a, ctx) => views.items(ex, a.collection, a.path, a.where, ctx)),
  route('selections', /^#\/selections(?:\?(.*))?$/, m => parseSelectionsQuery(m[1]), (ex, a, ctx) => views.selections(ex, a, ctx)),
  route('selection', /^#\/selection\/([^/?]+)$/, m => ({ cid: m[1] }), (ex, a, ctx) => views.selection(ex, a.cid, ctx)),
  // Projects and hand-off templates (P4).
  route('projects', /^#\/projects(?:\?(.*))?$/, m => parseProjectsQuery(m[1]), (ex, a, ctx) => views.projects(ex, ctx, a)),
  route('project', /^#\/project\/([^/?]+)(?:\?(.*))?$/, m => ({ cid: m[1], query: parseProjectQuery(m[2]) }),
    (ex, a, ctx) => views.project(ex, a.cid, ctx, a.query)),
  route('handoff', /^#\/handoff\/([^/?]+)(?:\?(.*))?$/, m => ({ template: decodeURIComponent(m[1]), query: parseHandoffQuery(m[2]) }),
    (ex, a, ctx) => views.handoffTemplate(ex, a.template, ctx, a.query)),
  // One store per page: ?store= names it for links from elsewhere, and the open member is the one shown (R9).
  route('storage', /^#\/storage(?:\?(.*))?$/, none, (ex, a, ctx) => views.storage(ex, ctx)),
  route('identity', /^#\/identity$/, none, (ex, a, ctx) => views.identity(ctx)),
  route('kit', /^#\/kit$/, none, () => kit()),
  route('compose', /^#\/compose$/, none, (ex, a, ctx) => { document.body.dataset.panel = 'open'; return views.compose(ex, ctx) }),
  route('search', /^#\/search(?:\?(.*))?$/, (m) => {
    const q = queryOf(m[1])
    return { q: q.get('q') ?? '', tab: q.get('tab') ?? null }
  }, (ex, a, ctx) => views.search(ex, a.q, ctx, { tab: a.tab })),
]

/** The route `hash` names, or null. */
export const routeOf = (hash) => ROUTES.find(r => r.pattern.test(hash)) ?? null

/** What `route`'s view takes from `hash`; throws as the route's own parsing does. */
export const argsOf = (route, hash) => route.args(hash.match(route.pattern))
