package robsyme.cas.trace

import java.nio.file.Files
import java.nio.file.Path

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import groovy.json.JsonSlurper
import nextflow.Global
import nextflow.Session
import nextflow.dataflow.ChannelNamespace
import nextflow.exception.AbortRunException
import nextflow.exception.MissingProcessException
import nextflow.processor.TaskProcessor
import nextflow.script.ProcessConfig
import nextflow.script.ScriptMeta
import nextflow.script.WorkflowMetadata
import nextflow.trace.event.FilePublishEvent
import nextflow.trace.event.TaskEvent
import nextflow.trace.event.WorkflowOutputEvent
import org.slf4j.LoggerFactory
import robsyme.cas.CasSession
import robsyme.cas.core.Cid
import robsyme.cas.core.DagCbor
import robsyme.cas.core.Index
import robsyme.cas.core.LocalRetentionStorage
import robsyme.cas.core.StoreLog
import robsyme.cas.core.StoreLogKind
import robsyme.cas.core.StoreRef
import robsyme.cas.core.SweepLock
import robsyme.cas.nio.CasFileSystemProvider
import robsyme.cas.s3.MemoryS3Ops
import robsyme.cas.s3.S3Ops
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
    /** The writable member's root, `tempDir/store` per {@link #config}. */
    Path storeRoot
    private final CasFileSystemProvider provider = new CasFileSystemProvider()

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
        storeRoot = tempDir.resolve('store')
    }

    private final List<ListAppender<ILoggingEvent>> appenders = []

    def cleanup() {
        appenders.each { ListAppender<ILoggingEvent> a -> ((Logger) LoggerFactory.getLogger(a.name)).detachAppender(a) }
        if( session != null )
            CasSession.unbind(session)
        Global.session = null
    }

    /**
     * The events of one logger: CasObserver's own by default, or {@code 'nextflow.cas'}, where
     * the fromStore hint goes so Nextflow's console filter shows it (LoggerHelper.groovy:408-441).
     */
    private ListAppender<ILoggingEvent> capture(String name = CasObserver.name) {
        final Logger logger = (Logger) LoggerFactory.getLogger(name)
        final ListAppender<ILoggingEvent> appender = new ListAppender<ILoggingEvent>()
        appender.name = name
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

    /** The {@code cas://} coordinate path for a URI, as the provider hands it to the observer. */
    private Path coord(String uri) {
        return provider.getPath(URI.create(uri))
    }

    /**
     * Publishes a file the way a {@code publishDir} or a workflow output
     * would: writes the bytes through the provider (hashing a block, writing
     * a Pointer File and recording the publish in {@code cas}), then fires the
     * {@link FilePublishEvent} the observer would receive. A second publish to
     * the same coordinate this run is tolerated (the pointer already exists);
     * either way the event fires so {@code publishedKeys} sees it.
     */
    private Path publish(String uri, String text) {
        final Path target = coord(uri)
        final Path source = Files.createTempFile(tempDir, 'publish', '.tmp')
        Files.writeString(source, text)
        try {
            provider.upload(source, target)
        }
        catch( java.nio.file.FileAlreadyExistsException ignored ) {
            // a second publish event for the same coordinate this run
        }
        observer.onFilePublish(new FilePublishEvent(null, target, null))
        return target
    }

    /**
     * Simulates a resumed run: the Pointer File exists from an earlier run
     * (written straight to the coordinate tree, bypassing {@code recordPublish})
     * so this run's {@code cas.publishes} names nothing for it.
     */
    private void writePointerOnly(String relPath, String text) {
        final byte[] bytes = text.getBytes('UTF-8')
        final Cid cid = cas.store.putStreaming(new ByteArrayInputStream(bytes))
        final int slash = relPath.lastIndexOf('/')
        final String name = slash < 0 ? relPath : relPath.substring(slash + 1)
        cas.coordinates.write(relPath, new StoreRef(cid, name))
    }

    /** The newest Store Log entry of kind RUN: the RunCompletion just written. */
    private Cid latestCompletion() {
        return StoreLog.read(cas.store).find { it.kind == StoreLogKind.RUN }.cid
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

    def 'with no resolvedConfig, the fallback config text is scrubbed of secrets, host paths and the user name'() {
        given: 'a session whose resolvedConfig is null (CmdRun sets it only when lineage.enabled is set)'
        final String user = System.getProperty('user.name')
        bind(config() + [
            env    : [FOO_API_KEY: 'sk-live-123'],
            aws    : [accessKey: 'AKIA1', secretKey: 'AWSSECRET1'],
            azure  : [storage: [accountKey: 'acct==']],
            process: [containerOptions: '--volume=/home/x/data:/data', clusterOptions: "--account=${user}".toString()],
        ])
        cas.setNextflowRunKey('nfhash123')

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        final String text = blocksOfKind('RunManifest')[0].get('config') as String

        then:
        text.contains('FOO_API_KEY')
        ['sk-live-123', 'AKIA1', 'AWSSECRET1', 'acct==', '/home/x', "--account=${user}".toString()].every { String leak -> !text.contains(leak) }
        text.contains('[secret]')
        text.contains('--volume=[redacted-location]:[redacted-location]')
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

    def 'a run registers in live/ at flow create and deregisters when its completion is written'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        final Path live = storeRoot.resolve('live')

        when:
        observer.onFlowCreate(session)

        then:
        Files.list(live).count() == 1
        new JsonSlurper().parse(live.resolve(session.uniqueId.toString())).session == session.uniqueId.toString()

        when:
        observer.onFlowComplete()

        then:
        Files.list(live).count() == 0
    }

    def 'a run waits while a fresh sweep lock is held, then proceeds'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        Files.createDirectories(storeRoot)
        SweepLock sweep = new SweepLock(new LocalRetentionStorage(storeRoot), 'sweep-9', { -> System.currentTimeMillis() } as Closure<Long>)
        sweep.take()
        int slept = 0
        CasSession.liveSleeper = { long ms -> if( ++slept == 1 ) sweep.release() } as Closure<Void>

        when:
        observer.onFlowCreate(session)

        then:
        slept == 1
        Files.exists(storeRoot.resolve('live').resolve(session.uniqueId.toString()))

        cleanup:
        CasSession.liveSleeper = { long ms -> Thread.sleep(ms) } as Closure<Void>
        observer.onFlowComplete()
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

    /**
     * A real ProcessConfig (its own afterScript, or none), since a mocked one
     * cannot satisfy TaskProcessor.getConfig()'s declared return type. A null
     * BaseScript defeats ProcessConfig's runtime constructor selection (a null
     * argument carries no runtime type, so Groovy's
     * ScriptBytecodeAdapter.selectConstructorAndTransformArguments picks the
     * (Map) constructor instead of (BaseScript, String), leaving
     * configProperties null) -- a Stub gives it a real type to match against.
     */
    private TaskProcessor process(String name, String afterScript = null) {
        final ProcessConfig pc = new ProcessConfig(Stub(nextflow.script.BaseScript), name)
        if( afterScript != null )
            pc.put('afterScript', afterScript)
        return Stub(TaskProcessor) {
            getName() >> name
            getConfig() >> pc
        }
    }

    def 'onProcessCreate warns once, on the console logger, when a process sets its own afterScript and node hashing is enabled'() {
        given:
        final Map cfg = config()
        ((Map) cfg.cas).nodeHash = true
        bind(cfg)
        final appender = capture('nextflow.cas')
        observer.onFlowCreate(session)

        when: 'created twice, as a process invoked more than once might be'
        observer.onProcessCreate(process('FOO', 'echo mine'))
        observer.onProcessCreate(process('FOO', 'echo mine'))

        then:
        warnings(appender, "process 'FOO' sets its own afterScript") == 1
    }

    def 'onProcessCreate says nothing when the process\'s afterScript is chained ahead by node hashing'() {
        given:
        final Map cfg = config()
        ((Map) cfg.cas).nodeHash = true
        bind(cfg)
        final appender = capture('nextflow.cas')
        observer.onFlowCreate(session)

        when:
        observer.onProcessCreate(process('BAR', NodeHash.script() + '\necho mine'))

        then:
        warnings(appender, 'BAR') == 0
    }

    def 'onProcessCreate says nothing when the process has no afterScript at all (the top-level default applies)'() {
        given:
        final Map cfg = config()
        ((Map) cfg.cas).nodeHash = true
        bind(cfg)
        final appender = capture('nextflow.cas')
        observer.onFlowCreate(session)

        when:
        observer.onProcessCreate(process('QUX', NodeHash.script()))

        then:
        warnings(appender, 'QUX') == 0
    }

    def 'onProcessCreate says nothing when node hashing is not enabled'() {
        given:
        bind(config())
        final appender = capture('nextflow.cas')
        observer.onFlowCreate(session)

        when:
        observer.onProcessCreate(process('BAZ', 'echo mine'))

        then:
        warnings(appender, 'BAZ') == 0
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

    def 'onFlowCreate aborts when the writable S3 member reports a clock 400 s away (ticket 03 decision 2)'() {
        given:
        MemoryS3Ops bucket = new MemoryS3Ops('member')
        bucket.serverDateMillis = System.currentTimeMillis() - 400_000L
        Closure<S3Ops> saved = CasSession.s3OpsFactory
        CasSession.s3OpsFactory = { Map c, String name -> bucket } as Closure<S3Ops>
        final Map cfg = config()
        ((Map) ((Map) cfg.cas).stores).lab = [location: 's3://member/cas']
        ((Map) cfg.cas).tmpDir = tempDir.resolve('t').toString()
        bind(cfg)

        when:
        observer.onFlowCreate(session)

        then:
        final AbortRunException e = thrown()
        e.message.contains('400 s ahead of')

        cleanup:
        CasSession.s3OpsFactory = saved
    }

    def 'a failed catch-up of the writable member keeps the old snapshot and logs catch_up_failed at info'() {
        given:
        bind(config())
        cas.setNextflowRunKey('nfhash123')
        session.isSuccess() >> true
        final Path snapshot = tempDir.resolve('store/index/v3.sqlite')
        Files.createDirectories(snapshot.parent)
        Files.write(snapshot, 'old'.bytes)
        final ListAppender<ILoggingEvent> logged = capture()
        ((Logger) LoggerFactory.getLogger(CasObserver.name)).level = Level.INFO
        Index failing = Spy(cas.openIndex()) {
            catchUp(_, _, 'lab', _, _, _) >> { throw new IOException('the Store Log cannot be listed') }
        }
        observer = new CasObserver() {
            @Override
            protected Index openIndex() { failing }
        }

        when:
        observer.onFlowCreate(session)
        observer.onFlowBegin()
        observer.onFlowComplete()

        then:
        new String(Files.readAllBytes(snapshot)) == 'old'
        logged.list.any { ILoggingEvent e -> e.level == Level.INFO && e.formattedMessage.contains('(catch_up_failed)') }
        blocksOfKind('RunCompletion').size() == 1

        cleanup:
        ((Logger) LoggerFactory.getLogger(CasObserver.name)).level = null
    }

    def 'onFlowError warns once with the include line for an untyped script missing it'() {
        given:
        bind(config())
        session.getError() >> missingFromStore('Channel.fromStore', Object)
        final appender = capture('nextflow.cas')
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
        final appender = capture('nextflow.cas')
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
        final appender = capture('nextflow.cas')
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
        final console = capture('nextflow.cas')
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
        warnings(console, FromStoreHint.UNTYPED) == 1
        cas.awaitCompletionWritten(0)

        cleanup:
        dirs.each { Path d -> d.toFile().setWritable(true, false) }
    }

    def 'a publish that joins to no workflow output counts as unjoined and is warned once'() {
        given:
        bind(config())
        final console = capture('nextflow.cas')
        observer.onFlowCreate(session)
        publish('cas://lab/legacy/A.txt', 'A\n')        // recordPublish + onFilePublish, as a publishDir would
        publish('cas://lab/legacy/B.txt', 'B\n')

        when:
        observer.onFlowComplete()

        then:
        final completion = readBlock(latestCompletion())
        completion.anomalies.unjoined == 2L
        warnings(console, '2 file(s) stored this run are in no workflow output') == 1
        console.list.find { it.formattedMessage.contains('stored this run') }.formattedMessage.contains('cas://lab/legacy/A.txt')
    }

    def 'a file stored with no publish event counts as unjoined (final review C1)'() {
        given: 'collectFile(storeDir:) uploads with no FilePublishEvent; an observer or script writes a stream'
        bind(config())
        final console = capture('nextflow.cas')
        observer.onFlowCreate(session)
        final Path source = Files.createTempFile(tempDir, 'collected', '.tmp')
        Files.writeString(source, 'A\nB\n')
        provider.upload(source, coord('cas://lab/collected/samples.txt'))
        coord('cas://lab/pipeline_info/versions.yml').text = 'v: 1\n'
        publish('cas://lab/legacy/A.txt', 'A\n')

        when:
        observer.onFlowComplete()

        then:
        readBlock(latestCompletion()).anomalies.unjoined == 3L
        warnings(console, '3 file(s) stored this run are in no workflow output') == 1
        console.list.find { it.formattedMessage.contains('stored this run') }.formattedMessage.contains('cas://lab/collected/samples.txt')
    }

    def 'a coordinate published by a workflow output is not unjoined even when publishDir also wrote it'() {
        given:
        bind(config())
        observer.onFlowCreate(session)
        final Path a = publish('cas://lab/aligned/A/A.bam', 'bam\n')
        publish('cas://lab/aligned/A/A.bam', 'bam\n')   // a second publish event for the same coordinate
        observer.onWorkflowOutput(new WorkflowOutputEvent('aligned', [[[id: 'A'], a]], null))

        when:
        observer.onFlowComplete()

        then:
        readBlock(latestCompletion()).anomalies.unjoined == 0L
    }

    def 'a publish known only by its pointer file still counts as unjoined'() {
        given: 'a resumed publishDir task: the pointer exists from an earlier run, and this run made no upload'
        bind(config())
        observer.onFlowCreate(session)
        writePointerOnly('legacy/C.txt', 'C\n')            // coordinates tree holds cas://<cid>/C.txt, no recordPublish
        observer.onFilePublish(new FilePublishEvent(null, coord('cas://lab/legacy/C.txt'), null))

        when:
        observer.onFlowComplete()

        then:
        readBlock(latestCompletion()).anomalies.unjoined == 1L
    }

    def 'an output with an index is recorded with its index leaf, and the index is not unjoined'() {
        given:
        bind(config())
        observer.onFlowCreate(session)
        final Path a = publish('cas://lab/tuples/A/A.txt', 'A\n')
        final Path idx = publish('cas://lab/tuples/index.json', '[]\n')
        observer.onWorkflowOutput(new WorkflowOutputEvent('tuples', [[[id: 'A'], a]], idx))

        when:
        observer.onFlowComplete()

        then:
        final completion = readBlock(latestCompletion())
        completion.anomalies.unjoined == 0L
        final collection = readBlock((Cid) (completion.collections as List)[0])
        (collection.index as Map).path == 'tuples/index.json'
    }

    def 'a CSV index written by appends is recorded as one leaf of its full bytes (Task 6a)'() {
        given: 'CsvWriter.apply at Nextflow 26.04.6: a delete, then one append per piece'
        bind(config())
        observer.onFlowCreate(session)
        final Path a = publish('cas://lab/records/A/A.txt', 'A\n')
        final Path idx = coord('cas://lab/records/index.csv')
        idx.delete()
        idx << '"id","file"' << '\n'
        idx << '"A","A.txt"' << '\n'
        observer.onFilePublish(new FilePublishEvent(null, idx, null))
        observer.onWorkflowOutput(new WorkflowOutputEvent('records', [[[id: 'A'], a]], idx))

        when:
        observer.onFlowComplete()

        then:
        final byte[] full = '"id","file"\n"A","A.txt"\n'.getBytes('UTF-8')
        final Cid expected = Cid.of(Cid.RAW, java.security.MessageDigest.getInstance('SHA-256').digest(full))
        final completion = readBlock(latestCompletion())
        completion.anomalies.unjoined == 0L
        final collection = readBlock((Cid) (completion.collections as List)[0])
        (collection.index as Map).path == 'records/index.csv'
        ((collection.index as Map).leaf as Map).address == expected
        cas.store.open(expected).withStream { it.bytes } == full
    }

    def 'an appended file with no publish event is still recorded when the run completes'() {
        given:
        bind(config())
        observer.onFlowCreate(session)
        coord('cas://lab/notes.txt') << 'one\n' << 'two\n'

        when:
        observer.onFlowComplete()

        then:
        final Cid expected = Cid.of(Cid.RAW, java.security.MessageDigest.getInstance('SHA-256').digest('one\ntwo\n'.getBytes('UTF-8')))
        cas.publishFor('cas://lab/notes.txt').ref.cid == expected
        cas.coordinates.read('notes.txt').get().cid == expected
    }

    def 'a file written after the run completes is stored at close (final review I2)'() {
        given: 'an observer after nf-blocks (nf-prov) writing into outputDir at onFlowComplete'
        bind(config())
        observer.onFlowCreate(session)
        observer.onFlowComplete()

        when:
        coord('cas://lab/pipeline_info/manifest.json').text = '{}\n'
        coord('cas://lab/pipeline_info/late.log') << 'one\n' << 'two\n'

        then:
        cas.coordinates.read('pipeline_info/manifest.json').get().cid == rawCidOf('{}\n')
        cas.coordinates.read('pipeline_info/late.log').get().cid == rawCidOf('one\ntwo\n')
        cas.publishFor('cas://lab/pipeline_info/late.log').size == 8
    }

    private static Cid rawCidOf(String text) {
        return Cid.of(Cid.RAW, java.security.MessageDigest.getInstance('SHA-256').digest(text.getBytes('UTF-8')))
    }

    def 'a file still being written when the run completes is left out of the run with a warning, and stored when it closes'() {
        given: 'a failed run whose completion races a publish thread mid-append'
        bind(config())
        final console = capture('nextflow.cas')
        observer.onFlowCreate(session)
        final Path a = publish('cas://lab/records/A/A.txt', 'A\n')
        final Path idx = coord('cas://lab/records/index.csv')
        final OutputStream open = provider.newOutputStream(idx, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND)
        open.write('"id"'.getBytes('UTF-8'))
        observer.onWorkflowOutput(new WorkflowOutputEvent('records', [[[id: 'A'], a]], idx))

        when:
        observer.onFlowComplete()

        then:
        final completion = readBlock(latestCompletion())
        (completion.collections as List).size() == 1
        cas.publishFor('cas://lab/records/index.csv') == null
        warnings(console, 'still open when the run completed') == 1
        console.list.find { it.formattedMessage.contains('still open') }.formattedMessage.contains('cas://lab/records/index.csv')

        when: 'the publish thread finishes after the join'
        open.write(',"file"\n'.getBytes('UTF-8'))
        open.close()

        then:
        cas.coordinates.read('records/index.csv').get().cid == rawCidOf('"id","file"\n')

        cleanup:
        open?.close()
    }

    def 'a process that declares publishDir is warned about once'() {
        given:
        bind(config())
        final console = capture('nextflow.cas')
        observer.onFlowCreate(session)
        final TaskProcessor p = Stub(TaskProcessor) {
            getName() >> 'LEGACY'
            getConfig() >> Stub(ProcessConfig) { get('publishDir') >> [[path: 'cas://lab/legacy']] }
        }

        when:
        observer.onProcessCreate(p)
        observer.onProcessCreate(p)

        then:
        warnings(console, "process 'LEGACY' uses publishDir") == 1
    }

    def 'a publishDir to a local path warns but counts nothing'() {
        given:
        bind(config())
        final console = capture('nextflow.cas')
        observer.onFlowCreate(session)
        final TaskProcessor p = Stub(TaskProcessor) {
            getName() >> 'LOCAL'
            getConfig() >> Stub(ProcessConfig) { get('publishDir') >> [[path: 'local_results']] }
        }
        observer.onProcessCreate(p)
        observer.onFilePublish(new FilePublishEvent(null, tempDir.resolve('local_results/x.txt'), null))

        when:
        observer.onFlowComplete()

        then:
        warnings(console, "process 'LOCAL' uses publishDir") == 1
        readBlock(latestCompletion()).anomalies.unjoined == 0L
    }
}
