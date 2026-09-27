package robsyme.cas.trace

import java.nio.file.Files
import java.nio.file.Path

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import nextflow.Global
import nextflow.Session
import nextflow.dataflow.ChannelNamespace
import nextflow.exception.AbortRunException
import nextflow.exception.MissingProcessException
import nextflow.script.ScriptMeta
import nextflow.script.WorkflowMetadata
import nextflow.trace.event.TaskEvent
import org.slf4j.LoggerFactory
import robsyme.cas.CasSession
import robsyme.cas.core.Cid
import robsyme.cas.core.DagCbor
import robsyme.cas.core.Index
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import spock.lang.Specification
import spock.lang.TempDir

/**
 * The observer is driven directly over a real {@link CasSession} (a real store
 * and coordinate tree under a temp dir) and a mock Nextflow {@link Session}.
 * Every assertion reads a real block back out of the store, never a mock.
 * DESIGN.md §11.
 */
class CasObserverTest extends Specification {

    @TempDir
    Path tempDir

    Session session
    CasSession cas
    CasObserver observer
    Path indexFile

    private Map config(String outputDir = 'cas://lab') {
        indexFile = tempDir.resolve('index.sqlite')
        return [
            lineage: [store: [location: 'cas://lab']],
            outputDir: outputDir,
            cas: [
                stores: [lab: [location: tempDir.resolve('store').toString()]],
                resolve: ['lab'],
                asserted_by: 'test',
                index: [path: indexFile.toString()],
            ],
        ]
    }

    /**
     * A metadata stub with the {@link java.time.OffsetDateTime} getters pinned
     * to null (OffsetDateTime cannot be mocked, so an auto-dummy would blow up)
     * -- the observer then falls back to "now" for the timestamps.
     */
    private WorkflowMetadata meta(Integer exitStatus = null) {
        return Stub(WorkflowMetadata) {
            getStart() >> null
            getComplete() >> null
            getManifest() >> null
            getNextflow() >> null
            getExitStatus() >> exitStatus
        }
    }

    private void bind(Map cfg, WorkflowMetadata metadata = meta()) {
        session = Mock(Session) {
            getConfig() >> cfg
            getRunName() >> 'test-run'
            getUniqueId() >> UUID.fromString('00000000-0000-0000-0000-0000000000ab')
            getWorkflowMetadata() >> metadata
        }
        Global.session = session
        cas = CasSession.of(session)
        observer = new CasObserver()
    }

    private final List<ListAppender<ILoggingEvent>> appenders = []

    def cleanup() {
        final Logger logger = (Logger) LoggerFactory.getLogger(CasObserver)
        appenders.each { logger.detachAppender(it) }
        if( session != null )
            CasSession.unbind(session)
        Global.session = null
    }

    private ListAppender<ILoggingEvent> capture() {
        final Logger logger = (Logger) LoggerFactory.getLogger(CasObserver)
        final ListAppender<ILoggingEvent> appender = new ListAppender<ILoggingEvent>()
        appender.start()
        logger.addAppender(appender)
        appenders << appender
        return appender
    }

    private static int warnings(ListAppender<ILoggingEvent> appender, String containing) {
        return appender.list.count { ILoggingEvent e -> e.level == Level.WARN && e.formattedMessage.contains(containing) } as int
    }

    /** The measured chain: MissingProcessException around the call's MissingMethodException. */
    private Throwable missingFromStore(String method, Class receiver) {
        final ScriptMeta meta = Stub(ScriptMeta) {
            getAllNames() >> new HashSet<String>()
        }
        final Object[] args = [[selection: 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua']] as Object[]
        return new MissingProcessException(meta, new MissingMethodException(method, receiver, args, receiver != Object))
    }

    private void makeBlocksUnwritable(List<Path> into) {
        final Path blocks = tempDir.resolve('store').resolve('blocks')
        into.addAll(Files.walk(blocks).filter { Path p -> Files.isDirectory(p) }.toList())
        into.each { Path d -> d.toFile().setWritable(false, false) }
    }

    private Map readBlock(Cid cid) {
        final InputStream input = cas.store.open(cid)
        try {
            return (Map) DagCbor.decode(input.readAllBytes())
        }
        finally {
            input.close()
        }
    }

    private List<Map> blocksOfKind(String kind) {
        final List<Map> found = new ArrayList<Map>()
        final stream = cas.store.listBlocks()
        try {
            for( Object element : stream.toList() ) {
                final Cid cid = (Cid) element
                if( !cid.isDagCbor() )
                    continue
                final Map block = readBlock(cid)
                if( block.get('kind') == kind )
                    found.add(block)
            }
        }
        finally {
            stream.close()
        }
        return found
    }

    def 'onFlowBegin writes a RunManifest with the nextflow run hash from the session'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()

        then:
        final manifests = blocksOfKind('RunManifest')
        manifests.size() == 1
        manifests[0].get('nf_run_hash') == 'nfhash123'
        manifests[0].get('asserted_by') == 'test'
        cas.getRunManifest() != null
    }

    def 'onFlowComplete twice writes exactly one RunCompletion and one Store Log entry'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()
        observer.onFlowComplete()

        then: 'the second call is a no-op (the completion latch already fired)'
        blocksOfKind('RunCompletion').size() == 1
        StoreLog.read(cas.store).size() == 1

        and: 'a successful run'
        blocksOfKind('RunCompletion')[0].get('status') == 'succeeded'
        blocksOfKind('RunCompletion')[0].get('possibly_incomplete') == false
    }


    def 'the notification that loses the completion latch waits for the winner to finish writing'() {
        // A failed run is notified twice: on the abort path (a finalizer thread)
        // and from Session.destroy on main, which reaches System.exit next. The
        // loser must not return, and let the JVM exit, while the winner writes.
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> false
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        and: 'the winner has claimed the latch and is still writing'
        cas.claimCompletion()

        when: 'the second notification arrives'
        final loser = Thread.start { observer.onFlowComplete() }
        loser.join(300)
        final boolean waitedForWinner = loser.isAlive()
        cas.completionWritten()
        loser.join(5_000)

        then:
        waitedForWinner
        !loser.isAlive()
    }

    def 'a failed run notified on an interrupted thread still writes its RunCompletion'() {
        // Nextflow shuts its executors down, interrupting their threads, while a
        // failed run is still notifying observers on one of them (measured in the
        // Gate's `fail` run: Session.shutdown0 interrupts TaskFinalizer-N mid-write).
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> false
        observer.onFlowCreate(session)
        observer.onFlowBegin()

        when: 'the notifying thread carries an interrupt'
        Thread.currentThread().interrupt()
        observer.onFlowComplete()
        final boolean stillInterrupted = Thread.interrupted()   // also clears it for the rest of the suite

        then: 'the provenance is written anyway, and the interrupt is handed back to the caller'
        blocksOfKind('RunCompletion').size() == 1
        blocksOfKind('RunCompletion')[0].get('status') == 'failed'
        StoreLog.read(cas.store).size() == 1
        stillInterrupted
    }

    def 'interrupts arriving while the completion is written do not lose it, and are handed back'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> false
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        boolean interruptedAfter = false
        Throwable thrown = null

        when: 'the notifying thread is interrupted over and over while it writes'
        final notifier = Thread.start {
            try { observer.onFlowComplete() } catch( Throwable t ) { thrown = t }
            interruptedAfter = Thread.currentThread().isInterrupted()
        }
        while( notifier.isAlive() ) {
            notifier.interrupt()
            Thread.sleep(1)
        }

        then:
        thrown == null
        blocksOfKind('RunCompletion').size() == 1
        interruptedAfter
    }

    def 'a failure while writing the completion reaches the caller and still releases the waiting notification'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> false
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        and: 'the block store can no longer be written'
        final blocks = tempDir.resolve('store').resolve('blocks')
        final List<Path> dirs = Files.walk(blocks).filter { Path p -> Files.isDirectory(p) }.toList()
        dirs.each { Path d -> d.toFile().setWritable(false, false) }

        when:
        observer.onFlowComplete()

        then:
        thrown(nextflow.exception.AbortRunException)
        cas.awaitCompletionWritten(0)

        cleanup:
        dirs.each { Path d -> d.toFile().setWritable(true, false) }
    }

    def 'the Store Log entry is stamped with the time it was written, not the run finish time'() {
        given: 'the usual binding, then an observer whose clock is fixed'
        final long fixed = 1_800_000_000_000L
        bind(config())
        observer = new CasObserver() {
            @Override
            protected long nowMillis() { return fixed }
        }
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()

        then:
        final entries = StoreLog.read(cas.store)
        entries.size() == 1
        entries[0].kind == StoreLogKind.RUN
        entries[0].writtenAtMillis == fixed
    }

    def 'a failed session yields a failed, possibly-incomplete RunCompletion'() {
        given:
        bind(config(), meta(7))
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> false

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()

        then:
        final completion = blocksOfKind('RunCompletion')[0]
        completion.get('status') == 'failed'
        completion.get('possibly_incomplete') == true
        completion.get('exit_status') == 7L
    }

    def 'the run is ingested into the index'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()

        then:
        final index = Index.open(indexFile)
        try {
            index.runByNextflowHash('nfhash123').isPresent()
        }
        finally {
            index.close()
        }
    }

    def 'an index failure is logged and does not propagate or lose the completion'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true
        // A subclass whose index cannot be opened, standing in for a broken index.
        observer = new CasObserver() {
            @Override
            protected Index openIndex() {
                throw new IllegalStateException('boom')
            }
        }

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()

        then: 'no exception, and the provenance block was still written'
        noExceptionThrown()
        blocksOfKind('RunCompletion').size() == 1
    }

    def 'a mismatched outputDir alias aborts at onFlowCreate'() {
        given:
        bind(config('cas://other'))

        when:
        observer.onFlowCreate(session)

        then:
        thrown(AbortRunException)
    }

    private static List<List<Object>> snapshotRows(Path db, String sql) {
        final def c = java.sql.DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            final def rs = c.createStatement().executeQuery(sql)
            final List<List<Object>> out = []
            while( rs.next() )
                out << [rs.getObject(1)]
            return out
        }
        finally {
            c.close()
        }
    }

    def 'onFlowComplete writes the writable member Index Snapshot after indexing'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()
        final Path snapshot = tempDir.resolve('store/index/v3.sqlite')

        then:
        Files.isRegularFile(snapshot)
        snapshotRows(snapshot, 'SELECT count(*) FROM run') == [[1]]
    }

    def 'a snapshot already over cas.snapshot.maxBytes is not rewritten by a run'() {
        given:
        final Map cfg = config()
        ((Map) cfg.cas).snapshot = [maxBytes: 1]
        bind(cfg)
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true
        final Path snapshot = tempDir.resolve('store/index/v3.sqlite')
        Files.createDirectories(snapshot.parent)
        Files.write(snapshot, 'old'.bytes)

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()

        then:
        new String(Files.readAllBytes(snapshot)) == 'old'
    }

    def 'a snapshot failure does not fail the run'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true
        // A directory where the snapshot file must go makes the atomic move fail.
        Files.createDirectories(tempDir.resolve('store/index/v3.sqlite/blocker'))

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()

        then:
        noExceptionThrown()
        blocksOfKind('RunCompletion').size() == 1
    }

    def 'onFlowError warns once with the include line for an untyped script missing it'() {
        given:
        bind(config())
        session.getError() >> missingFromStore('Channel.fromStore', Object)
        final appender = capture()
        observer.onFlowCreate(session)

        when: 'notified twice, as a later task error would'
        observer.onFlowError(new TaskEvent(null, null))
        observer.onFlowError(new TaskEvent(null, null))

        then:
        warnings(appender, FromStoreHint.UNTYPED) == 1
    }

    def 'onFlowError gives a typed script the nextflow.Channel workaround'() {
        given:
        bind(config())
        session.getError() >> missingFromStore('fromStore', ChannelNamespace)
        final appender = capture()
        observer.onFlowCreate(session)

        when:
        observer.onFlowError(new TaskEvent(null, null))

        then:
        warnings(appender, FromStoreHint.TYPED) == 1
    }

    def 'onFlowError says nothing about fromStore for any other error, or none'() {
        given:
        bind(config())
        session.getError() >> error
        final appender = capture()
        observer.onFlowCreate(session)

        when:
        observer.onFlowError(new TaskEvent(null, null))

        then:
        warnings(appender, 'fromStore') == 0

        where:
        error << [null, new IllegalStateException('Process `HASH` terminated with an error exit status (1)')]
    }

    def 'a failing run still gets its failed RunCompletion when the store is fine'() {
        given:
        bind(config(), meta(1))
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> false
        session.getError() >> missingFromStore('Channel.fromStore', Object)

        when:
        observer.onFlowCreate(session)
        observer.onFlowComplete()

        then:
        noExceptionThrown()
        blocksOfKind('RunCompletion').size() == 1
        blocksOfKind('RunCompletion')[0].get('status') == 'failed'
    }

    def 'on a run that is already failing, a provenance write failure is logged, not thrown, so onFlowError still runs'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> false
        session.getError() >> missingFromStore('Channel.fromStore', Object)
        final appender = capture()
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        final List<Path> dirs = []
        makeBlocksUnwritable(dirs)

        when: 'the order Session.abort uses (Session.groovy:830-831)'
        observer.onFlowComplete()
        observer.onFlowError(new TaskEvent(null, null))

        then:
        noExceptionThrown()
        warnings(appender, 'the run is already failing') == 1
        warnings(appender, FromStoreHint.UNTYPED) == 1
        cas.awaitCompletionWritten(0)

        cleanup:
        dirs.each { Path d -> d.toFile().setWritable(true, false) }
    }
}
