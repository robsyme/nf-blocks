package robsyme.cas

import java.nio.file.Path

import robsyme.cas.core.*
import robsyme.cas.s3.*
import spock.lang.Specification
import spock.lang.TempDir

/** A composition with a writable S3 member, on the test double (no AWS). */
class CasSessionS3Test extends Specification {

    @TempDir Path tmp
    Map<String, MemoryS3Ops> buckets = [:]
    Closure<S3Ops> saved

    def setup() {
        saved = CasSession.s3OpsFactory
        CasSession.s3OpsFactory = { Map config, String bucket -> buckets.computeIfAbsent(bucket) { new MemoryS3Ops(bucket) } } as Closure<S3Ops>
    }

    def cleanup() { CasSession.s3OpsFactory = saved }

    private CasConfig config(Map extra = [:]) {
        final Map cfg = [cas: [stores: [lab: [location: 's3://member/cas'], shared: [location: tmp.resolve('shared').toString()]],
                               index: [path: tmp.resolve('cache.sqlite').toString()], tmpDir: tmp.resolve('t').toString()]]
        cfg.putAll(extra)
        return CasConfig.from(cfg, 'cas://lab')
    }

    private CasSession session(Map extra = [:]) {
        return new CasSession(config(extra))
    }

    /** Runs {@code body} with the nextflow.cas logger captured; returns its lines as LEVEL message. */
    private static List<String> consoleDuring(Closure body) {
        final def logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger('nextflow.cas')
        final def level = logger.level
        logger.level = ch.qos.logback.classic.Level.INFO
        final def appender = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>()
        appender.start()
        logger.addAppender(appender)
        try {
            body.call()
        }
        finally {
            logger.detachAppender(appender)
            logger.level = level
        }
        return appender.list.collect { "${it.level} ${it.formattedMessage}".toString() }
    }

    def 'members are built from config: S3 writable first, local read-only after'() {
        when:
        final CasSession s = session()

        then:
        s.members()*.class == [S3BlockStore, LocalBlockStore]
        s.members()[0].isWritable() && !s.members()[1].isWritable()
        s.coordinatesOf('lab') instanceof S3CoordinateTree
        s.coordinatesOf('shared') instanceof LocalCoordinateTree
        s.snapshotsOf('lab') instanceof S3SnapshotStorage
    }

    def 'a Selection put through the session lands in the bucket with its Store Log entry, and the snapshot follows'() {
        given:
        final CasSession s = session()
        final Cid item = s.members()[0].putDagCbor(Fixtures.outputItem2([[sample: 'A']]))
        final Index index = s.openIndex()

        when:
        final PutResult r = s.newPut(index).put(DagJson.encode([kind: 'Selection', members: [[item: [address: item, via: []]]], derived_from: []]), false)
        final SnapshotBase base = s.snapshotBase()
        final Set<String> failed = s.catchUpIndex(index)
        final IndexSnapshot.Result snap = s.snapshotWritable(index, 0L, base, failed)

        then:
        buckets['member'].objects.keySet().any { it.startsWith('cas/log/') && it.contains('-selection-') }
        snap.written
        buckets['member'].objects['cas/index/v7.sqlite'].metadata.runs == '0'

        cleanup:
        index?.close()
    }

    def 'a failed catch-up of the writable member keeps the old snapshot (catch_up_failed)'() {
        given:
        final CasSession s = session()
        final Index index = s.openIndex()

        expect:
        s.snapshotWritable(index, 0L, s.snapshotBase(), ['lab'] as Set).skipped == IndexSnapshot.CATCH_UP_FAILED

        cleanup:
        index?.close()
    }

    def 'a snapshot replaced between the HEAD and the GET that counts it gives no base, and no throw (Task 7 carry)'() {
        given: 'a stored snapshot without x-amz-meta-runs, so base() must GET it with If-Match'
        buckets['member'] = new MemoryS3Ops('member') {
            @Override InputStream get(String key, String ifMatch, long start, long length) {
                if( ifMatch != null ) throw new S3PreconditionFailed("${key} is no longer ${ifMatch}")
                return super.get(key, ifMatch, start, length)
            }
        }
        buckets['member'].putText('cas/' + IndexSnapshot.relativePath(), 'not counted')
        final CasSession s = session()

        expect:
        s.snapshotBase() == null
    }

    def 'a page PUT that fails after the snapshot is written warns and the write still counts (rule 3)'() {
        given:
        buckets['member'] = new MemoryS3Ops('member') {
            @Override S3Written put(String key, S3Body body, S3PutOptions o) {
                if( key.endsWith(IndexSnapshot.PAGE_NAME) ) throw new IOException('403 Forbidden')
                return super.put(key, body, o)
            }
        }
        final CasSession s = session()
        final Index index = s.openIndex()

        when:
        final SnapshotBase base = s.snapshotBase()
        IndexSnapshot.Result r = s.snapshotWritable(index, 0L, base, s.catchUpIndex(index))

        then:
        noExceptionThrown()
        r.written
        buckets['member'].objects.containsKey('cas/' + IndexSnapshot.relativePath())
        !buckets['member'].objects.containsKey('cas/' + IndexSnapshot.PAGE_NAME)

        cleanup:
        index?.close()
    }

    def 'the clock check: a skew over 5 minutes throws'() {
        given:
        final CasSession s = session()
        buckets['member'].serverDateMillis = System.currentTimeMillis() - 400_000L

        when:
        s.checkClock()

        then:
        final ClockSkewException e = thrown()
        e.message.contains('behind') || e.message.contains('ahead of')
    }

    def 'the clock check: a skew over 1 minute and under 5 does not throw'() {
        given:
        final CasSession s = session()
        buckets['member'].serverDateMillis = System.currentTimeMillis() - 90_000L

        when:
        s.checkClock()

        then:
        noExceptionThrown()
        buckets['member'].calls.contains('HEAD cas/' + IndexSnapshot.relativePath())
    }

    def 'the clock check: a HEAD that fails (403, network) warns and continues'() {
        given:
        buckets['member'] = new MemoryS3Ops('member') {
            @Override S3Head head(String key) { throw new IOException('403 Forbidden') }
        }
        final CasSession s = session()

        when:
        s.checkClock()

        then:
        noExceptionThrown()
    }

    def 'the clock check: a local writable member makes no request'() {
        given:
        final CasSession s = new CasSession(CasConfig.from([cas: [stores: [lab: [location: tmp.resolve('local').toString()]],
            index: [path: tmp.resolve('local.sqlite').toString()]]], 'cas://lab'))

        when:
        s.checkClock()

        then:
        noExceptionThrown()
        buckets.isEmpty()
    }

    def 'the cache file is named by the location texts, S3 ones included'() {
        expect:
        session().config.locationTexts() == ['s3://member/cas', tmp.resolve('shared').toString()]
    }

    def 'a command session sends its storage-class and clock-skew notices to its err and nothing to nextflow.cas'() {
        given:
        final ByteArrayOutputStream err = new ByteArrayOutputStream()
        buckets['member'] = new MemoryS3Ops('member')
        buckets['member'].serverDateMillis = System.currentTimeMillis() - 90_000L
        CasSession s = null

        when:
        final List<String> console = consoleDuring {
            s = CasSession.forCommand(config([aws: [client: [storageClass: 'STANDARD_IA']]]), new PrintStream(err, true, 'UTF-8'))
            s.checkClock()
        }
        final List<String> lines = err.toString('UTF-8').readLines()

        then:
        console.isEmpty()
        lines.size() == 2
        lines[0] == s.config.storageClassWarning
        lines[0].contains('packing')
        lines[1].contains('behind') || lines[1].contains('ahead of')
    }

    def 'a command session whose clock check cannot reach S3 says so on its err'() {
        given:
        final ByteArrayOutputStream err = new ByteArrayOutputStream()
        buckets['member'] = new MemoryS3Ops('member') {
            @Override S3Head head(String key) { throw new IOException('403 Forbidden') }
        }

        when:
        final List<String> console = consoleDuring {
            CasSession.forCommand(config(), new PrintStream(err, true, 'UTF-8')).checkClock()
        }

        then:
        console.isEmpty()
        err.toString('UTF-8').readLines() == ["could not check this machine's clock against S3 (403 Forbidden); continuing"]
    }

    def 'a run session sends its storage-class and clock-skew notices to nextflow.cas, once each'() {
        given:
        buckets['member'] = new MemoryS3Ops('member')
        buckets['member'].serverDateMillis = System.currentTimeMillis() - 90_000L
        CasSession s = null

        when:
        final List<String> console = consoleDuring {
            s = session([aws: [client: [storageClass: 'STANDARD_IA']]])
            session([aws: [client: [storageClass: 'STANDARD_IA']]])
            s.checkClock()
        }

        then:
        console.size() == 2
        console[0] == "WARN ${s.config.storageClassWarning}".toString()
        console[1].startsWith('WARN ')
        console[1].contains('behind') || console[1].contains('ahead of')
    }
}
