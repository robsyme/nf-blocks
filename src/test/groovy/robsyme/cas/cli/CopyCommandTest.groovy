package robsyme.cas.cli

import java.nio.file.Files
import java.nio.file.Path

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import robsyme.cas.CopyService
import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.Selection
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

/** `nf-blocks:copy` flags, exit codes and output (P3 plan 1.1), driven through `CasCommands.run`. */
class CopyCommandTest extends Specification {

    @TempDir Path tmp

    LocalBlockStore lab
    long now = 1_760_000_000_000L
    Cid file, item, manifest, collection, completion, sel, leakySel
    ByteArrayOutputStream out = new ByteArrayOutputStream()
    ByteArrayOutputStream err = new ByteArrayOutputStream()

    def setup() {
        lab = new LocalBlockStore(tmp.resolve('lab'), 'lab', true)
        file = lab.putStreaming(new ByteArrayInputStream('counts'.getBytes('UTF-8')))
        item = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's1'], counts: Fixtures.leaf2('c.tsv', file, 6)]))
        manifest = lab.putDagCbor(Fixtures.runManifest(run_name: 'one'))
        collection = lab.putDagCbor(Fixtures.outputCollection(manifest, 'counts', [[item, ['c.tsv']]]))
        completion = logged(StoreLogKind.RUN, Fixtures.runCompletion(manifest, [collection]))
        sel = logged(StoreLogKind.SELECTION, new Selection('ada', [Selection.item(item, [collection])], []).toCbor())

        final Cid leaky = lab.putDagCbor(Fixtures.runManifest(run_name: 'two', params: [outdir: '/home/someone/out']))
        final Cid other = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's2'], counts: Fixtures.leaf2('c.tsv', file, 6)]))
        final Cid otherC = lab.putDagCbor(Fixtures.outputCollection(leaky, 'counts', [[other, ['c.tsv']]]))
        logged(StoreLogKind.RUN, Fixtures.runCompletion(leaky, [otherC]))
        leakySel = logged(StoreLogKind.SELECTION, new Selection('ada', [Selection.item(other, [otherC])], []).toCbor())
    }

    private Cid logged(StoreLogKind kind, Map block) {
        final Cid cid = lab.putDagCbor(block)
        StoreLog.append(lab, kind, cid, now += 1000)
        return cid
    }

    private Map config() {
        return [lineage: [store: [location: 'cas://lab']],
                cas    : [stores     : [lab: [location: tmp.resolve('lab').toString()],
                                        org: [location: tmp.resolve('org').toString()],
                                        ro : [location: tmp.resolve('ro').toString(), readOnly: true]],
                          asserted_by: 'copier',
                          index      : [path: tmp.resolve('cache/index.sqlite').toString()]]]
    }

    private int run(List<String> args) {
        final CasCommands commands = new CasCommands()
        commands.clock = { -> now += 1000 } as Closure<Long>
        return commands.run('copy', args, config(), new PrintStream(out, true), new PrintStream(err, true))
    }

    def 'a dry run prints the plan line and exits 0; a bare --dry-run arrives as --dry-run true'() {
        when:
        final int code = run(["selection:${sel}".toString(), '--to', 'org', '--dry-run', 'true'])
        final String text = out.toString()

        then:
        code == 0
        text.startsWith("copy selection:${sel} from lab to org, depth results, claims names,projects,annotations (dry run, nothing written)")
        text.contains('6 blocks \u00b7 0 already in org \u00b7 6 to transfer \u00b7 ')
        text.contains(' \u00b7 0 inputs referenced but not stored (stay as references)\n')
        text.contains('  run_records ')
        text.contains('audit: 1 RunManifest checked, no findings')
        !Files.exists(tmp.resolve('org'))
    }

    def '--format json prints the 1.3 body, the same the service returns'() {
        given:
        final Map request = [root: "selection:${sel}".toString(), from: null, to: 'org', depth: 'lineage', claims: ['names'], accept_audit: false]
        final Map direct = new CopyService().run(config(), request, true, new PrintStream(new ByteArrayOutputStream(), true)).body

        when:
        final int code = run(["selection:${sel}".toString(), '--to', 'org', '--depth', 'lineage', '--claims', 'names', '--dry-run', 'true', '--format', 'json'])
        final Map printed = (Map) new JsonSlurper().parseText(out.toString())

        then:
        code == 0
        out.toString().readLines().size() == 1
        printed == new JsonSlurper().parseText(JsonOutput.toJson(direct))
        printed.depth == 'lineage'
        printed.command == "nextflow plugin nf-blocks:copy selection:${sel} --from lab --to org --depth lineage --claims names --dry-run".toString()
    }

    def 'a copy exits 0 and says what it wrote'() {
        when:
        final int code = run(["selection:${sel}".toString(), '--to', 'org'])
        final List lines = out.toString().readLines()

        then:
        code == 0
        lines.last() ==~ /copied 6 blocks \(.+\); Transfer \S+; snapshot written/
        StoreLog.read(new LocalBlockStore(tmp.resolve('org'), 'org', false))[0].kind == StoreLogKind.TRANSFER
    }

    def 'audit findings: a dry run lists them and exits 0; a copy exits 1 unless --accept-audit true'() {
        when:
        final int dry = run(["selection:${leakySel}".toString(), '--to', 'org', '--dry-run', 'true'])

        then:
        dry == 0
        out.toString().contains('audit: 1 RunManifest checked, 1 finding')
        out.toString().contains(' params.outdir: ')

        when:
        err.reset()
        final int refused = run(["selection:${leakySel}".toString(), '--to', 'org'])

        then:
        refused == 1
        err.toString().contains('nf-blocks:copy: the audit found 1 finding(s)')
        !Files.exists(tmp.resolve('org'))

        when:
        final int accepted = run(["selection:${leakySel}".toString(), '--to', 'org', '--accept-audit', 'true'])

        then:
        accepted == 0
        Files.exists(tmp.resolve('org/log'))
    }

    def 'D9: a copy of a run the destination released exits 1, names it on stderr and writes nothing'() {
        given:
        run(["selection:${sel}".toString(), '--to', 'org'])
        final LocalBlockStore org = new LocalBlockStore(tmp.resolve('org'), 'org', true)
        final Cid release = org.putDagCbor(Fixtures.claim(completion, 'set', 'retain', 'lineage', []))
        StoreLog.append(org, StoreLogKind.CLAIM, release, now += 1000)
        final int entries = StoreLog.read(org).size()
        out.reset()

        when:
        final int dry = run(["run:${completion}".toString(), '--to', 'org', '--dry-run', 'true'])

        then:
        dry == 0
        out.toString().contains("in org: root one ${completion} is released")
        !out.toString().contains("in org: run one")

        when:
        err.reset()
        final int code = run(["run:${completion}".toString(), '--to', 'org'])

        then:
        code == 1
        err.toString().contains("nf-blocks:copy: 'org' would not keep this copy; nothing was copied. Restore these there first, then copy again:")
        err.toString().contains("  root one ${completion}: released in org")
        and: 'the way out is the page route, with a config file of its own so pipelines keep writing where they do'
        // NXF_CONFIG_FILE replaces ./nextflow.config, so the explorer's own file must bring the stores along.
        err.toString().contains("To restore it, make a config file beside your nextflow.config holding includeConfig 'nextflow.config' " +
            "(or your cas.stores block) and lineage.store.location = 'cas://org', start nextflow plugin nf-blocks:explore " +
            "with NXF_CONFIG_FILE=<that file>, and restore it on its page. " +
            "Don't put the lineage line in the config your pipelines use: they would start writing into org.")
        StoreLog.read(org).size() == entries
    }

    def 'exit codes: usage errors are 2, a refused target is 1'() {
        expect:
        run(args) == code
        err.toString().contains(message)

        where:
        args                                                              || code | message
        []                                                                || 2    | 'copy takes one root'
        ['selection:x', 'selection:y', '--to', 'org']                     || 2    | 'copy takes one root'
        ['selection:x']                                                   || 2    | 'copy needs --to <alias>'
        ['selection:x', '--to', 'org', '--format', 'yaml']                || 2    | '--format is text or json'
        ['selection:x', '--to', 'org', '--dry-run', 'maybe']              || 2    | '--dry-run is true or false'
        ['selection:x', '--to', 'org', '--wat', '1']                      || 2    | 'unknown option --wat'
        ['selection:x', '--to', 'org']                                    || 2    | "does not end in a content address"
        ['nope', '--to', 'org']                                           || 2    | 'a copy root is selection:<cid>'
        ['run:latest', '--to', 'org']                                     || 2    | "'latest' needs a pipeline, which copy has no flag for"
        ['selection:x', '--to', 'elsewhere']                              || 2    | "'elsewhere' is not a configured store"
        ['selection:x', '--to', 'org', '--from', 'org']                   || 2    | 'a copy goes between two members'
        ['selection:x', '--to', 'org', '--depth', 'deep']                 || 2    | '--depth is one of results, lineage, all'
        ['selection:x', '--to', 'org', '--claims', 'none,pins']           || 2    | '--claims none stands alone'
        ['selection:x', '--to', 'ro']                                     || 1    | "'ro' is not a copy target: readOnly is set for this store"
    }

    def 'a copy into a member that already holds it all exits 0 and says no Transfer was written'() {
        given:
        run(["selection:${sel}".toString(), '--to', 'org'])
        out.reset()

        when:
        final int code = run(["selection:${sel}".toString(), '--to', 'org'])

        then:
        code == 0
        out.toString().readLines().last() == 'copied 0 blocks (0 B); org already held all of it, so no Transfer was written'
    }

    def 'a readOnly on the target that is not true or false is exit 1'() {
        given:
        final Map cfg = config()
        ((Map) ((Map) cfg.cas).stores).org = [location: tmp.resolve('org').toString(), readOnly: 'yes']

        when:
        final int code = new CasCommands().run('copy', ["selection:${sel}".toString(), '--to', 'org', '--format', 'json'], cfg,
            new PrintStream(out, true), new PrintStream(err, true))

        then:
        code == 1
        ((Map) new JsonSlurper().parseText(out.toString())).error == 'bad_config'
        err.toString().contains("cas.stores.org.readOnly must be true or false, got 'yes'")
    }

    def 'a root no member holds is exit 1'() {
        when:
        final int code = run(["selection:${Fixtures.contentCid('gone')}".toString(), '--to', 'org', '--format', 'json'])
        final Map printed = (Map) new JsonSlurper().parseText(out.toString())

        then:
        code == 1
        printed.error == 'root_not_found'
    }

    def 'a copy that fails part way exits 1 and prints the failure'() {
        given: 'the file in lab no longer hashes to its address'
        lab.blockPath(file).toFile().setWritable(true)
        Files.write(lab.blockPath(file), 'COUNTS'.getBytes('UTF-8'))

        when:
        final int code = run(["selection:${sel}".toString(), '--to', 'org'])

        then:
        code == 1
        out.toString().contains("  failed ${file}: ")
        err.toString().contains('no Transfer was written')
    }
}
