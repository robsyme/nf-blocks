package robsyme.cas.trace

import java.nio.file.Path

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import nextflow.Global
import nextflow.Session
import nextflow.exception.AbortRunException
import org.slf4j.LoggerFactory
import robsyme.cas.CasPlugin
import robsyme.cas.CasSession
import robsyme.cas.nio.CasFileSystemProvider
import robsyme.cas.nio.CasPath
import spock.lang.Specification
import spock.lang.TempDir

/**
 * Ticket 02: an unset outputDir defaults to the lineage alias, set in the
 * factory so WorkflowMetadata (Session.groovy:472) copies the default. Every
 * other case is left for CasObserver.onFlowCreate to judge as before.
 */
class CasObserverFactoryTest extends Specification {

    @TempDir
    Path tempDir

    Session session
    /** What the mock session's outputDir property holds; [0] is the value. */
    Path[] box
    Logger logger
    Level savedLevel
    ListAppender<ILoggingEvent> appender

    def setupSpec() {
        // FileHelper.toCanonicalPath('cas://lab') reaches CasPathFactory and
        // CasPlugin.provider(); prime the provider as CasLinStoreTest does.
        final field = CasPlugin.getDeclaredField('provider')
        field.setAccessible(true)
        field.set(null, new CasFileSystemProvider())
    }

    def setup() {
        logger = (Logger) LoggerFactory.getLogger(CasObserverFactory)
        savedLevel = logger.level
        logger.level = Level.INFO
        appender = new ListAppender<ILoggingEvent>()
        appender.start()
        logger.addAppender(appender)
    }

    def cleanup() {
        logger.detachAppender(appender)
        logger.level = savedLevel
        if( session != null )
            CasSession.unbind(session)
        Global.session = null
    }

    private Map config(Map overrides = [:]) {
        final Map cfg = [
            lineage: [enabled: true, store: [location: 'cas://lab']],
            cas: [
                stores: [lab: [location: tempDir.resolve('store').toString()]],
                resolve: ['lab'],
                asserted_by: 'test',
                index: [path: tempDir.resolve('index.sqlite').toString()],
            ],
        ]
        cfg.putAll(overrides)
        return cfg
    }

    /**
     * A mock session whose outputDir behaves as the real property does. The
     * holder is a local so the mock's configuration closure cannot resolve it
     * against the mock itself.
     */
    private void bind(Map cfg, Path initial) {
        final Path[] holder = [initial] as Path[]
        session = Mock(Session) {
            getConfig() >> cfg
            getOutputDir() >> { holder[0] }
            setOutputDir(_) >> { Path p -> holder[0] = p }
            getRunName() >> 'test-run'
        }
        box = holder
        Global.session = session
    }

    private boolean logged(String text) {
        return appender.list.any { ILoggingEvent e -> e.level == Level.INFO && e.formattedMessage == text }
    }

    def 'an unset outputDir becomes the lineage alias, logged at info, and the config is left as written'() {
        given: 'what Session.create sets for an unset outputDir (Session.groovy:418)'
        bind(config(), tempDir.resolve('results'))

        when:
        CasObserverFactory.defaultOutputDir(session)

        then:
        box[0] instanceof CasPath
        box[0].toString() == 'cas://lab'
        logged('outputDir not set; publishing to cas://lab')
        !session.config.containsKey('outputDir')
    }

    def 'nothing changes when #why'() {
        given:
        final Path initial = tempDir.resolve('results')
        bind(config(overrides), initial)

        when:
        CasObserverFactory.defaultOutputDir(session)

        then:
        box[0] == initial
        appender.list.every { ILoggingEvent e -> !e.formattedMessage.contains('outputDir not set') }

        where:
        why                                                   | overrides
        'lineage is off'                                      | [lineage: [enabled: false, store: [location: 'cas://lab']]]
        'lineage.enabled is absent'                           | [lineage: [store: [location: 'cas://lab']]]
        'there is no lineage scope'                           | [lineage: null]
        'the lineage store is not a cas:// location'          | [lineage: [enabled: true, store: [location: 'file:///tmp/lineage']]]
        'the lineage store has no location'                   | [lineage: [enabled: true]]
        'the lineage location is not a bare alias'            | [lineage: [enabled: true, store: [location: 'cas://lab/sub']]]
        'the lineage location has an empty alias'             | [lineage: [enabled: true, store: [location: 'cas://']]]
        'outputDir is a local path'                           | [outputDir: 'results']
        'outputDir already names the alias'                   | [outputDir: 'cas://lab']
        'outputDir came from -output-dir (ConfigBuilder:570)' | [outputDir: '/data/out']
        'outputDir is a directory inside the alias'           | [outputDir: 'cas://lab/sub']
        'outputDir names another alias'                       | [outputDir: 'cas://other']
    }

    def 'create defaults the outputDir first, and the observer then accepts the run at onFlowCreate'() {
        given:
        bind(config(), tempDir.resolve('results'))
        final List<Object> observers = []

        when:
        observers.addAll(new CasObserverFactory().create(session))
        (observers[0] as CasObserver).onFlowCreate(session)

        then:
        noExceptionThrown()
        observers.size() == 1
        observers[0] instanceof CasObserver
        box[0].toString() == 'cas://lab'
    }

    def 'an explicit #value is left alone and still aborts at onFlowCreate (Review Focus 3)'() {
        given:
        bind(config(outputDir: value), tempDir.resolve('explicit'))
        final CasObserver observer = new CasObserverFactory().create(session)[0] as CasObserver

        when:
        observer.onFlowCreate(session)

        then:
        final AbortRunException e = thrown()
        e.message == "outputDir must publish through the lineage store 'cas://lab', but is '${value}'".toString()
        box[0] == tempDir.resolve('explicit')

        where:
        value << ['cas://lab/sub', '/data/out', 'cas://other']
    }

    def 'with lineage off, an unset outputDir is not defaulted and onFlowCreate still aborts naming it unset'() {
        given:
        bind(config(lineage: [store: [location: 'cas://lab']]), tempDir.resolve('results'))
        final CasObserver observer = new CasObserverFactory().create(session)[0] as CasObserver

        when:
        observer.onFlowCreate(session)

        then:
        final AbortRunException e = thrown()
        e.message == "outputDir must publish through the lineage store 'cas://lab', but is 'unset'"
    }
}
