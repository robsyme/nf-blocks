package robsyme.cas

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

import robsyme.cas.cli.CasCommands
import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.Index
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.Selection
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogEntry
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

/**
 * A v4 build from before the transfer tables (270f03a) drops `transfer`
 * Store Log entries as an unknown kind but still moves its watermark past
 * them, and its snapshots carry neither the tables nor a `transfer`
 * log_entry row. This build recreates such a cache and does not seed from
 * such a snapshot (schema 6), so the member is read again from its blocks
 * and the D8 presence check sees the Transfer (integrator review REV6).
 *
 * The CopyServiceTest layout: `lab` holds a run, and `sel` = {c1 via counts}
 * is copied to `org`, which then holds c1's files counts1 (9 B) and f1 (2 B).
 * An org entry an hour after the copy's puts the watermark well past the
 * 10-minute overlap before the transfer entry.
 */
class TransferAfterV4Test extends Specification {

    @TempDir Path tmp

    LocalBlockStore lab
    long now = 1_760_000_000_000L
    Closure<Long> clock = { -> now += 1000 } as Closure<Long>

    Cid counts1, counts2, bam1, f1, script, dir1, c1, c2, i1, manifest, countsC, bamC, completion, sel

    def setup() {
        lab = new LocalBlockStore(tmp.resolve('lab'), 'lab', true)
        counts1 = raw('counts-s1'); counts2 = raw('counts-s2'); bam1 = raw('bam-s1'); f1 = raw('f1'); script = raw('workflow { }')
        dir1 = lab.putDagCbor([kind: 'DirectoryManifest', schema: 2, entries: [[name: 'f1', mode: 'regular', size: 2, address: f1, target: null]]])
        c1 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's1'], counts: Fixtures.leaf2('c.tsv', counts1, 9), dir: Fixtures.leaf2('d', dir1, 0)]))
        c2 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's2'], counts: Fixtures.leaf2('c2.tsv', counts2, 9)]))
        i1 = lab.putDagCbor(Fixtures.outputItem2([meta: [id: 's1'], bam: Fixtures.leaf2('a.bam', bam1, 6)]))
        manifest = lab.putDagCbor(Fixtures.runManifest(run_name: 'one', pipeline: 'p1', nf_run_hash: 'unknown', script: script))
        countsC = lab.putDagCbor(Fixtures.outputCollection(manifest, 'counts', [[c1, ['c.tsv', 'd']], [c2, ['c2.tsv']]]))
        bamC = lab.putDagCbor(Fixtures.outputCollection(manifest, 'bam', [[i1, ['a.bam']]]))
        completion = logged(StoreLogKind.RUN, Fixtures.runCompletion(manifest, [bamC, countsC]))
        sel = logged(StoreLogKind.SELECTION, new Selection('ada', [Selection.item(c1, [countsC])], []).toCbor())
        logged(StoreLogKind.CLAIM, Fixtures.claim(sel, 'set', 'name', 'wigshop', []))
    }

    private Cid raw(String text) {
        return lab.putStreaming(new ByteArrayInputStream(text.getBytes('UTF-8')))
    }

    private Cid logged(StoreLogKind kind, Map block) {
        final Cid cid = lab.putDagCbor(block)
        StoreLog.append(lab, kind, cid, now += 1000)
        return cid
    }

    private Map copyConfig() {
        return [lineage: [store: [location: 'cas://lab']],
                cas    : [stores: [lab: [location: tmp.resolve('lab').toString()], org: [location: tmp.resolve('org').toString()]],
                          resolve: ['lab'], asserted_by: 'copier',
                          index: [path: tmp.resolve('copy-cache.sqlite').toString()], tmpDir: tmp.resolve('t').toString()]]
    }

    /** org alone, against the cache at {@code cache}: what `nf-blocks:snapshot` of org reads. */
    private Map orgConfig(Path cache) {
        return [lineage: [store: [location: 'cas://org']],
                cas    : [stores: [org: [location: tmp.resolve('org').toString()]], asserted_by: 'rev',
                          index: [path: cache.toString()], tmpDir: tmp.resolve('t').toString()]]
    }

    private static List<List<Object>> rows(Path db, String sql) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            final ResultSet rs = c.createStatement().executeQuery(sql)
            final List<List<Object>> out = []
            while( rs.next() )
                out << (1..rs.metaData.columnCount).collect { int i -> rs.getObject(i) }
            return out
        }
        finally {
            c.close()
        }
    }

    private static void exec(Path db, List<String> statements) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            for( String sql : statements )
                c.createStatement().executeUpdate(sql)
        }
        finally {
            c.close()
        }
    }

    /** What a v4 build from before P3 leaves: schema version 4, no transfer tables, and no `transfer` log_entry or missing row. */
    private static void asV4BeforeP3(Path db) {
        exec(db, ['DROP TABLE transfer_root', 'DROP TABLE transfer',
                  "DELETE FROM missing WHERE needed_cid IN (SELECT cid FROM log_entry WHERE kind = 'transfer')",
                  "DELETE FROM log_entry WHERE kind = 'transfer'",
                  'UPDATE schema_version SET version = 4'])
        assert rows(db, "SELECT name FROM sqlite_master WHERE name LIKE 'transfer%'") == []
    }

    /** Copies sel from lab to org, then logs an org Claim an hour after the transfer entry. The Transfer's cid. */
    private Cid copiedThenAnHourLater() {
        final CopyService.Outcome o = new CopyService(clock).run(copyConfig(), [root: "selection:${sel}".toString(), to: 'org'], false,
            new PrintStream(new ByteArrayOutputStream(), true))
        assert o.status == 200
        final LocalBlockStore org = new LocalBlockStore(tmp.resolve('org'), 'org', true)
        final StoreLogEntry t = StoreLog.read(org).find { it.kind == StoreLogKind.TRANSFER }
        assert t.cid.toString() == o.body.transfer_entry
        final Cid later = org.putDagCbor(Fixtures.claim(sel, 'add', 'annotation', 'later note', []))
        StoreLog.append(org, StoreLogKind.CLAIM, later, t.writtenAtMillis + 3_600_000L)
        return t.cid
    }

    private int snapshotOrg(Path cache) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream(), err = new ByteArrayOutputStream()
        final int code = new CasCommands().run('snapshot', [], orgConfig(cache), new PrintStream(out, true), new PrintStream(err, true))
        assert code == 0, err.toString('UTF-8')
        return code
    }

    private Path orgSnapshot() { tmp.resolve('org').resolve(IndexSnapshot.relativePath()) }

    private void expectTransferIn(Path cache, Cid transfer) {
        assert rows(cache, 'SELECT cid FROM transfer') == [[transfer.toString()]]
        assert rows(cache, "SELECT role FROM transfer_root WHERE transfer_cid = '${transfer}' ORDER BY role") == [['root'], ['run']]
        assert rows(cache, "SELECT kind, member FROM log_entry WHERE cid = '${transfer}'") == [['transfer', 'org']]
        final Path snap = orgSnapshot()
        assert rows(snap, 'SELECT cid FROM transfer') == [[transfer.toString()]]
        assert rows(snap, 'SELECT role, count(*) FROM transfer_root GROUP BY role ORDER BY role') == [['root', 1], ['run', 1]]
    }

    def 'a v4 cache a pre-P3 build caught up past the transfer entry is recreated, and org counts only what it received'() {
        given: 'lab and org caught up by a v4 build that skipped the transfer entry and moved its watermark an hour past it'
        final Cid transfer = copiedThenAnHourLater()
        final Path cache = tmp.resolve('old-cache.sqlite')
        final Index old = Index.open(cache)
        for( String m : ['lab', 'org'] ) {
            final LocalBlockStore s = new LocalBlockStore(tmp.resolve(m), m, false)
            old.catchUp(s, StoreLog.of(s), m)
        }
        old.close()
        asV4BeforeP3(cache)
        assert rows(cache, "SELECT value FROM meta WHERE key = 'store_log_watermark:org'")[0][0] ==
            StoreLog.read(new LocalBlockStore(tmp.resolve('org'), 'org', false))[0].name

        when: 'this build snapshots org from that cache'
        snapshotOrg(cache)

        then: 'the cache was recreated, and it and the snapshot hold the Transfer, so org counts counts1 and f1 only'
        rows(cache, 'SELECT version FROM schema_version') == [[Index.SCHEMA_VERSION]]
        rows(orgSnapshot(), 'SELECT stored_bytes, stored_blocks FROM store_stats') == [[11, 2]]
        expectTransferIn(cache, transfer)
    }

    def 'a cold cache does not seed from a v4 pre-P3 snapshot of org, gains the Transfer, and org counts only what it received'() {
        given: 'a snapshot of org taken after the later entry, as a v4 build before P3 wrote it'
        final Cid transfer = copiedThenAnHourLater()
        snapshotOrg(tmp.resolve('p3-cache.sqlite'))
        asV4BeforeP3(orgSnapshot())
        final String wm = rows(orgSnapshot(), "SELECT value FROM meta WHERE key = '${IndexSnapshot.WATERMARK_KEY}'")[0][0]
        assert wm == StoreLog.read(new LocalBlockStore(tmp.resolve('org'), 'org', false))[0].name

        when: 'a cold cache catches org up and snapshots it'
        final Path cache = tmp.resolve('cold.sqlite')
        snapshotOrg(cache)

        then: 'not seeded: the cold cache read every block'
        rows(cache, "SELECT count(*) FROM meta WHERE key = 'seeded_from:org'") == [[0]]
        rows(orgSnapshot(), 'SELECT version FROM schema_version') == [[Index.SCHEMA_VERSION]]
        rows(orgSnapshot(), 'SELECT stored_bytes, stored_blocks FROM store_stats') == [[11, 2]]
        expectTransferIn(cache, transfer)
    }
}
