package robsyme.cas.cli

import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import robsyme.cas.CopyService
import robsyme.cas.core.SweepReport

/**
 * `nextflow plugin nf-blocks:copy <root> --to <alias>` (P3 plan 1.1): the
 * flags become a {@link CopyService} request, and its status an exit code.
 * 200 is 0; a 400 that says the request was malformed is 2 (usage) and any
 * other 400 is 1, as are 409 (a copy the destination would not keep, D9, or
 * audit findings) and 500 (a failed copy).
 * `--format json` prints the 1.3 body, or the 400 body, on stdout.
 */
@CompileStatic
class CopyCommand {

    static final Set<String> FLAGS = Collections.unmodifiableSet(
        ['to', 'from', 'depth', 'claims', 'dry-run', 'accept-audit', 'format'] as Set<String>)

    private static final String DOT = ' \u00b7 '

    static int run(Options o, Map config, PrintStream out, PrintStream err, Closure<Long> clock) {
        if( o.positionals.size() != 1 )
            throw new UsageException("copy takes one root (selection:<cid>, run:<ref> or a cid), got ${o.positionals ?: 'none'}")
        final boolean dryRun = bool(o, 'dry-run')
        final boolean accept = bool(o, 'accept-audit')
        final String format = o.flag('format') ?: 'text'
        if( !(format in ['text', 'json']) )
            throw new UsageException("--format is text or json, got '${format}'")
        if( o.flag('to') == null )
            throw new UsageException('copy needs --to <alias>, the member to copy into')

        final Map<String, Object> request = new LinkedHashMap<String, Object>()
        request.put('root', o.positionals[0])
        request.put('from', o.flag('from'))
        request.put('to', o.flag('to'))
        request.put('depth', o.flag('depth'))
        request.put('claims', o.flag('claims') == null ? null : o.flag('claims').split(',', -1).collect { String c -> c.trim() })
        request.put('accept_audit', accept)

        final CopyService.Outcome outcome = new CopyService(clock).run(config, request, dryRun, err)
        final Map<String, Object> body = outcome.body
        if( format == 'json' )
            out.println(JsonOutput.toJson(body))
        switch( outcome.status ) {
            case CopyService.BAD_REQUEST:
                err.println("nf-blocks:copy: ${body.get('message')}")
                return body.get('error') in CopyService.USAGE_ERRORS ? 2 : 1
            case CopyService.REFUSED:
                if( format == 'text' )
                    out.print(text(body))
                if( body.get('refused') == CopyService.NOT_KEPT ) {
                    final List<Map> gone = ((List<Map>) plan(body).get('destination_state'))
                        .findAll { Map d -> d.get('state') in [CopyService.RELEASED, CopyService.DELETED] }
                    err.println("nf-blocks:copy: '${body.get('to')}' would not keep this copy; nothing was copied. " +
                        "Restore these there first, then copy again:")
                    for( Map d : gone )
                        err.println("  ${d.get('role')} ${d.get('name') ?: 'unnamed'} ${d.get('cid')}: ${d.get('state')} in ${body.get('to')}")
                    // The same route as the copy page's note: restore through the explorer opened on the
                    // destination, with a config of its own, so no pipeline starts writing there.
                    // NXF_CONFIG_FILE replaces ./nextflow.config, so that file must bring the stores along.
                    final String to = body.get('to')
                    final String them = gone.size() == 1 ? 'it' : 'them'
                    final String pages = gone.size() == 1 ? 'its page' : 'their pages'
                    err.println("To restore ${them}, make a config file beside your nextflow.config holding includeConfig 'nextflow.config' " +
                        "(or your cas.stores block) and lineage.store.location = 'cas://${to}', start nextflow plugin nf-blocks:explore " +
                        "with NXF_CONFIG_FILE=<that file>, and restore ${them} on ${pages}. " +
                        "Don't put the lineage line in the config your pipelines use: they would start writing into ${to}.")
                    return 1
                }
                final int findings = ((List) ((Map) plan(body).get('audit')).get('findings')).size()
                err.println("nf-blocks:copy: the audit found ${findings} " +
                    'finding(s) in the RunManifests that would travel; nothing was copied. Copy again with --accept-audit true to copy them as they are')
                return 1
            case CopyService.FAILED:
                if( format == 'text' )
                    out.print(text(body))
                err.println("nf-blocks:copy: the copy stopped after ${body.get('copied')} block(s); no Transfer was written")
                return 1
            default:
                if( format == 'text' )
                    out.print(text(body))
                return 0
        }
    }

    private static boolean bool(Options o, String name) {
        final String value = o.flag(name)
        if( !(value in [null, 'true', 'false']) )
            throw new UsageException("--${name} is true or false, got '${value}'")
        return value == 'true'
    }

    private static Map plan(Map body) { (Map) body.get('plan') }

    private static long num(Map m, String key) {
        final Object v = m.get(key)
        return v instanceof Number ? ((Number) v).longValue() : 0L
    }

    /** The plan as design doc §7 words it, one line per group, the audit, and what a copy did. */
    static String text(Map body) {
        final StringBuilder out = new StringBuilder()
        final Map plan = plan(body)
        final String to = body.get('to')
        final Map root = (Map) body.get('root')
        final List claims = (List) body.get('claims')
        out << "copy ${root.get('kind')}:${root.get('cid')} from ${body.get('from')} to ${to}, depth ${body.get('depth')}, " +
            "claims ${claims ? claims.join(',') : 'none'}${body.get('dry_run') ? ' (dry run, nothing written)' : ''}\n"
        final String atLeast = num(plan, 'unsized') > 0 ? 'at least ' : ''
        out << "${SweepReport.count(num(plan, 'blocks'))} blocks${DOT}${SweepReport.count(num(plan, 'present'))} already in ${to}${DOT}" +
            "${SweepReport.count(num(plan, 'transfer'))} to transfer${DOT}${atLeast}${SweepReport.size(num(plan, 'transfer_bytes'))}${DOT}" +
            "${SweepReport.count(num(plan, 'unaddressed'))} inputs referenced but not stored (stay as references)\n"
        if( num(plan, 'missing_at_source') > 0 )
            out << "${SweepReport.count(num(plan, 'missing_at_source'))} blocks the closure names are not in ${body.get('from')}; they stay as references\n"
        for( Object each : (List) plan.get('groups') ) {
            final Map g = (Map) each
            out << String.format(Locale.ROOT, '  %-12s %s', g.get('group'),
                "${SweepReport.count(num(g, 'blocks'))} ${SweepReport.plural(num(g, 'blocks'), 'block')} (${SweepReport.size(num(g, 'bytes'))})${DOT}" +
                "${SweepReport.count(num(g, 'present'))} already in ${to}${DOT}${SweepReport.count(num(g, 'transfer'))} to transfer " +
                "(${SweepReport.size(num(g, 'transfer_bytes'))})") << '\n'
        }
        for( Object each : (List) plan.get('runs') ) {
            final Map r = (Map) each
            out << "  run ${r.get('run_name') ?: 'unnamed'}${r.get('pipeline') ? " (${r.get('pipeline')})" : ''} ${r.get('completion')}" +
                "${r.get('present') ? ", already in ${to}" : ''}${r.get('lineage') ? ", lineage by ${r.get('lineage')}" : ''}\n"
        }
        final Map skipped = (Map) plan.get('skipped_claims')
        final int notCarried = ((List) plan.get('claims')).findAll { Object c -> !((Map) c).get('carried') }.size()
        out << "  claims ${((List) plan.get('claims')).size() - notCarried} carried, ${notCarried} not asked for, " +
            "${num(skipped, 'retain')} retain and ${num(skipped, 'deletion')} deletion that never travel\n"
        final Map audit = (Map) plan.get('audit')
        final List findings = (List) audit.get('findings')
        out << "audit: ${num(audit, 'checked')} ${SweepReport.plural(num(audit, 'checked'), 'RunManifest')} checked, " +
            "${findings ? "${findings.size()} ${SweepReport.plural(findings.size(), 'finding')}" : 'no findings'}\n"
        for( Object each : findings ) {
            final Map f = (Map) each
            out << "  ${f.get('cid')} ${f.get('where')}: ${f.get('what')}\n"
        }
        // D9: what the destination's own Claims say of each root and run it already knows.
        for( Object each : (List) plan.get('destination_state') ?: Collections.emptyList() ) {
            final Map d = (Map) each
            if( d.get('state') != CopyService.ABSENT )
                out << "in ${to}: ${d.get('role')} ${d.get('name') ?: 'unnamed'} ${d.get('cid')} is ${d.get('state')}\n"
        }
        if( body.containsKey('copied') ) {
            out << "copied ${SweepReport.count(num(body, 'copied'))} ${SweepReport.plural(num(body, 'copied'), 'block')} " +
                "(${SweepReport.size(num(body, 'copied_bytes'))})"
            if( body.get('transfer_entry') )
                out << "; Transfer ${body.get('transfer_entry')}; snapshot ${body.get('snapshot')}"
            else if( !body.get('failures') )
                out << "; ${to} already held all of it, so no Transfer was written"
            out << '\n'
            for( Object each : (List) body.get('failures') ) {
                final Map f = (Map) each
                out << "  failed ${f.get('cid')}: ${f.get('error')}\n"
            }
        }
        return out.toString()
    }
}
