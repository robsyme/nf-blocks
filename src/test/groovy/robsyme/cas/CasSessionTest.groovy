package robsyme.cas

import java.nio.file.Path
import java.sql.DriverManager
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory
import robsyme.cas.core.Cid
import robsyme.cas.core.Fixtures
import robsyme.cas.core.Index
import robsyme.cas.core.IndexSnapshot
import robsyme.cas.core.LocalBlockStore
import robsyme.cas.core.RetentionFixture
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

/**
 * {@link CasSession} behaviour not tied to a particular store transport
 * (see {@link CasSessionS3Test} for the S3-composition cases).
 */
class CasSessionTest extends Specification {

    @TempDir
    Path tmp

    CasSession cas

    /** What reached the nextflow.cas logger, the one Nextflow's console shows. */
    ListAppender<ILoggingEvent> console
    Logger consoleLogger
    Level consoleLevel

    AtomicInteger gitCalls = new AtomicInteger()
    AtomicInteger platformCalls = new AtomicInteger()

    def setup() {
        CasSession.platformIdentity = { String e, String t -> platformCalls.incrementAndGet(); throw new IOException('no platform in tests') } as PlatformIdentity
        CasSession.gitEmail = { -> gitCalls.incrementAndGet(); ' someone@example.org\n' } as GitEmail
        CasSession.env = [:]
        cas = new CasSession(cfg('store'))
        consoleLogger = (Logger) LoggerFactory.getLogger('nextflow.cas')
        consoleLevel = consoleLogger.level
        consoleLogger.level = Level.INFO
        console = new ListAppender<ILoggingEvent>()
        console.start()
        consoleLogger.addAppender(console)
    }

    def cleanup() {
        consoleLogger.detachAppender(console)
        consoleLogger.level = consoleLevel
    }

    private CasConfig cfg(String name, Map extra = [:]) {
        return CasConfig.from([cas: [stores: [lab: [location: tmp.resolve(name).toString()]],
                                     index: [path: tmp.resolve("${name}.sqlite").toString()]] + extra], 'cas://lab')
    }

    private static PrintStream stream(ByteArrayOutputStream bytes) { new PrintStream(bytes, true, 'UTF-8') }

    def 'with no asserted_by the session resolves a principal from git through the seams'() {
        expect:
        cas.assertedBy == 'someone@example.org'
        cas.principal.source == 'git'
    }

    def 'a command session that never signs calls no identity seam; the first assertedBy resolves once'() {
        given:
        CasSession.env = [TOWER_ACCESS_TOKEN: 'tok']
        final ByteArrayOutputStream err = new ByteArrayOutputStream()

        when: 'a read-only verb builds its session and reads the store and config'
        CasSession session = CasSession.forCommand(cfg('ro'), stream(err))
        session.store
        session.config
        session.members()
        session.openIndex().close()

        then:
        platformCalls.get() == 0
        gitCalls.get() == 0
        err.toString('UTF-8') == ''

        when: 'a verb that signs reads assertedBy, twice'
        final String first = session.assertedBy
        final String second = session.assertedBy

        then:
        first == 'someone@example.org'
        second == first
        platformCalls.get() == 1
        gitCalls.get() == 1
    }

    def 'eight threads reading assertedBy at once resolve once'() {
        given:
        CasSession.gitEmail = { -> gitCalls.incrementAndGet(); Thread.sleep(200); 'someone@example.org' } as GitEmail
        CasSession session = CasSession.forCommand(cfg('race'), stream(new ByteArrayOutputStream()))
        CountDownLatch go = new CountDownLatch(1)
        def pool = Executors.newFixedThreadPool(8)

        when:
        def answers = (1..8).collect { pool.submit({ -> go.await(); session.assertedBy } as java.util.concurrent.Callable<String>) }
        go.countDown()
        List<String> names = answers*.get(5, TimeUnit.SECONDS)

        then:
        names.unique() == ['someone@example.org']
        gitCalls.get() == 1

        cleanup:
        pool.shutdownNow()
    }

    def 'a run session prints the signing line to nextflow.cas once, when it first resolves'() {
        when: 'two run sessions are built and neither signs yet'
        final CasSession a = new CasSession(cfg('s2'))
        final CasSession b = new CasSession(cfg('s2'))

        then:
        console.list.isEmpty()

        when: 'both sign'
        a.assertedBy
        b.assertedBy

        then:
        console.list*.formattedMessage == ['nf-blocks: signing as someone@example.org (from git config user.email)']
        console.list[0].level == Level.INFO
    }

    def 'a command session prints the signing line and the anonymous warning to its err, never to nextflow.cas'() {
        given:
        final ByteArrayOutputStream err = new ByteArrayOutputStream()

        when: 'a command session signs as a name'
        final CasSession named = CasSession.forCommand(cfg('s3'), stream(err))
        named.assertedBy
        named.assertedBy

        then:
        err.toString('UTF-8').readLines() == ['nf-blocks: signing as someone@example.org (from git config user.email)']
        console.list.isEmpty()

        when: 'a command session finds no identity'
        err.reset()
        CasSession.gitEmail = { -> '' } as GitEmail
        final CasSession anonymous = CasSession.forCommand(cfg('s3'), stream(err))

        then:
        anonymous.assertedBy == 'anonymous'
        err.toString('UTF-8').readLines() == ["nf-blocks: no identity is configured, so what this command writes will say 'anonymous'. " +
            'Set NXF_BLOCKS_USER or git config user.email (see docs/identity.md)']
        console.list.isEmpty()
    }

    /** One run logged in the local member at {@code tmp/<name>}. */
    private void logRun(String name, String run) {
        final LocalBlockStore lab = new LocalBlockStore(tmp.resolve(name), 'lab', true)
        final Cid m = lab.putDagCbor(Fixtures.runManifest(run_name: run, nf_run_hash: run))
        final Cid rc = lab.putDagCbor(Fixtures.runCompletion(m, []))
        StoreLog.append(lab, StoreLogKind.RUN, rc, System.currentTimeMillis())
    }

    def 'a command session sends the index lines to its err, never to nextflow.cas'() {
        given: 'a local member with a Store Log entry and no Index Snapshot, and a fresh cache'
        logRun('cold', 'r1')
        final ByteArrayOutputStream err = new ByteArrayOutputStream()

        when: 'a command catches the cache up'
        final CasSession first = CasSession.forCommand(cfg('cold'), stream(err))
        Index index = first.openIndex()
        first.catchUpIndex(index)
        index.close()

        then:
        err.toString('UTF-8').readLines() == ["store member 'lab' has no usable Index Snapshot (absent); indexing it from every block " +
            'instead, which is slow on a large member. `nextflow plugin nf-blocks:snapshot` against it writes one.']
        console.list.isEmpty()

        when: 'the member is recreated at the same location and another command catches up'
        err.reset()
        tmp.resolve('cold').toFile().deleteDir()
        logRun('cold', 'r2')
        final CasSession second = CasSession.forCommand(cfg('cold'), stream(err))
        index = second.openIndex()
        second.catchUpIndex(index)
        index.close()

        then:
        err.toString('UTF-8').readLines().any { it.startsWith('rebuilding the cache index ') && it.contains("store member 'lab' was replaced") }
        console.list.isEmpty()
    }

    def 'a Platform failure that leaves nothing else prints one combined warning (#kind session)'() {
        given:
        CasSession.platformIdentity = { String e, String t -> throw new IOException('HTTP 401') } as PlatformIdentity
        CasSession.gitEmail = { -> '' } as GitEmail
        CasSession.env = [TOWER_ACCESS_TOKEN: 'secret-tok']
        final ByteArrayOutputStream err = new ByteArrayOutputStream()

        when:
        CasSession session = kind == 'command' ? CasSession.forCommand(cfg('pf'), stream(err)) : new CasSession(cfg('pf'))
        session.assertedBy
        final List<String> lines = kind == 'command' ? err.toString('UTF-8').readLines() : console.list*.formattedMessage

        then:
        lines == ["nf-blocks: could not read your Seqera Platform identity (HTTP 401) and no other identity is configured, " +
            "so ${what} will say 'anonymous'. Set NXF_BLOCKS_USER or git config user.email (see docs/identity.md)".toString()]
        !lines[0].contains('secret-tok')
        kind == 'command' ? console.list.isEmpty() : console.list[0].level == Level.WARN

        where:
        kind      | what
        'command' | 'what this command writes'
        'run'     | "this run's blocks and claims"
    }

    def 'the global seams: with TOWER_ACCESS_TOKEN in the process env a session without its own seams is anonymous and Platform is never asked'() {
        given: 'what the extension installs before every spec, over a process env that carries a token'
        IdentitySeamsExtension.Saved saved = IdentitySeamsExtension.install(
            [TOWER_ACCESS_TOKEN: 'tok', TOWER_API_ENDPOINT: 'https://api.cloud.seqera.io', NXF_BLOCKS_USER: 'dev', HOME: '/home/x'])
        final int before = IdentitySeamsExtension.platformCalls.get()

        when:
        CasSession session = new CasSession(cfg('global'))

        then:
        session.assertedBy == 'anonymous'
        session.principal.source == 'anonymous'
        IdentitySeamsExtension.platformCalls.get() == before
        CasSession.env == [HOME: '/home/x']
        !(CasSession.gitEmail instanceof ProcessGitEmail)
        !(CasSession.platformIdentity instanceof HttpPlatformIdentity)

        cleanup:
        IdentitySeamsExtension.restore(saved)
    }

    def 'the global seams are in place in a spec that sets none'() {
        expect: 'this spec sets its own in setup, so look at what the extension put back under them'
        IdentitySeamsExtension.cleanEnv(System.getenv()).keySet().every { !it.startsWith('TOWER_') && it != 'NXF_BLOCKS_USER' }
        IdentitySeamsExtension.cleanEnv([TOWER_ACCESS_TOKEN: 't', TOWER_WORKSPACE_ID: '1', NXF_BLOCKS_USER: 'u', PATH: '/bin']) == [PATH: '/bin']
    }

    def 'a read-only-only session has nothing to register; stop is idempotent'() {
        when:
        cas.stopLiveWriter()
        cas.stopLiveWriter()

        then:
        notThrown(Exception)
    }

    private static String graceIn(Path store) {
        DriverManager.getConnection("jdbc:sqlite:${IndexSnapshot.pathIn(store)}").withCloseable { c ->
            final def rs = c.createStatement().executeQuery("SELECT value FROM meta WHERE key = '${IndexSnapshot.SWEEP_GRACE_KEY}'")
            rs.next() ? rs.getString(1) : null
        }
    }

    def 'the writable snapshot carries cas.sweep.grace in ms, 14 days by default (P2 ruling R7)'() {
        given:
        final CasSession three = new CasSession(CasConfig.from([
            cas: [
                stores: [lab: [location: tmp.resolve('three').toString()]],
                index : [path: tmp.resolve('three.sqlite').toString()],
                sweep : [grace: '3d'],
            ],
        ], 'cas://lab'))
        Index index = cas.openIndex()
        Index other = three.openIndex()

        when:
        cas.snapshotWritable(index, 0L, cas.snapshotBase(), cas.catchUpIndex(index))
        three.snapshotWritable(other, 0L, three.snapshotBase(), three.catchUpIndex(other))

        then:
        graceIn(tmp.resolve('store')) == '1209600000'
        graceIn(tmp.resolve('three')) == '259200000'

        cleanup:
        index?.close(); other?.close()
    }

    def 'a cas.sweep.grace that does not parse leaves the key out of the snapshot, with one warning'() {
        given: 'a run session: its notices reach the nextflow.cas console logger (P1b)'
        final CasSession bad = new CasSession(CasConfig.from([
            cas: [
                stores: [lab: [location: tmp.resolve('bad').toString()]],
                index : [path: tmp.resolve('bad.sqlite').toString()],
                sweep : [grace: 'soon'],
            ],
        ], 'cas://lab'))
        Index index = bad.openIndex()

        when:
        bad.snapshotWritable(index, 0L, bad.snapshotBase(), bad.catchUpIndex(index))
        bad.snapshotWritable(index, 0L, bad.snapshotBase(), bad.catchUpIndex(index), true)

        then:
        graceIn(tmp.resolve('bad')) == null
        console.list.count { it.formattedMessage.contains("cas.sweep.grace is a duration such as '14d'") } == 1

        cleanup:
        index?.close()
    }

    def 'the writable snapshot leaves out a block a sweep took from a released run (fix round 2, B1)'() {
        given: 'run a holds shared; run b, released, held shared and only, and only is gone from the store'
        final RetentionFixture f = new RetentionFixture(tmp.resolve('store'))
        final Cid shared = f.raw('shared')
        final Cid only = f.raw('only b')
        f.run('a', [[s: shared]])
        final Map b = f.run('b', [[s: shared, u: only]])
        f.claim((Cid) b.completion, 'set', 'retain', 'lineage')
        java.nio.file.Files.delete(((LocalBlockStore) f.store).blockPath(only))
        Index index = cas.openIndex()

        when:
        final IndexSnapshot.Result r = cas.snapshotWritable(index, 0L, cas.snapshotBase(), cas.catchUpIndex(index))

        then:
        r.written
        DriverManager.getConnection("jdbc:sqlite:${IndexSnapshot.pathIn(tmp.resolve('store'))}").withCloseable { c ->
            final def rs = c.createStatement().executeQuery('SELECT stored_bytes, releasing_bytes FROM store_stats')
            rs.next()
            [rs.getLong(1), rs.getLong(2)]
        } == [1L, 0L]
        index.absentBlocks('lab') == [only.toString()] as Set

        cleanup:
        index?.close()
    }
}
