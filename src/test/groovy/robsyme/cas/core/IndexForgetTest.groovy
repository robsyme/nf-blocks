package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path

import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §20 decision 8, plan decision 9. */
class IndexForgetTest extends Specification {

    @TempDir Path tmp
    RetentionFixture f
    Index index

    def setup() {
        f = new RetentionFixture(tmp.resolve('lab'))
        index = Index.open(tmp.resolve('cache.sqlite'))
    }

    def cleanup() { index?.close() }

    private void catchUp(BlockStore holders = f.store) {
        index.catchUp(f.store, StoreLog.of(f.store), 'lab', null, tmp, Collections.<String> emptySet(), holders)
    }

    /** Deletes the blocks the way a sweep does and logs one forget entry for them. */
    private Cid sweepAway(List<Cid> cids) {
        for( Cid c : cids )
            Files.deleteIfExists(((LocalBlockStore) f.store).blockPath(c))
        final Cid forget = f.store.putDagCbor(new Forget('test', cids).toCbor())
        StoreLog.append(f.store, StoreLogKind.FORGET, forget, ++f.clock)
        return forget
    }

    def 'a forget entry removes the rows of what it lists'() {
        given:
        final Map gone = f.run('gone', [[g: f.raw('g')]])
        final Map kept = f.run('kept', [[k: f.raw('k')]])
        catchUp()

        when:
        sweepAway([gone.completion, gone.collection] + gone.items as List<Cid>)
        catchUp()

        then:
        !index.isRunIndexed((Cid) gone.completion)
        index.isRunIndexed((Cid) kept.completion)
        index.forgottenRunsSince('lab', null) == 1
    }

    def 'a listed cid the store holds again is not forgotten'() {
        given:
        final Map run = f.run('r', [[x: f.raw('x')]])
        catchUp()
        final Cid item = ((List<Cid>) run.items)[0]
        final byte[] bytes = Files.readAllBytes(((LocalBlockStore) f.store).blockPath(item))
        sweepAway([item])
        f.store.put(item, new ByteArrayInputStream(bytes), bytes.length)

        when:
        catchUp()

        then:
        index.items((Cid) run.completion, 'out', [:]) == [item]
    }

    def 'a listed cid another member holds is not forgotten'() {
        given:
        final Map run = f.run('r', [[x: f.raw('x')]])
        catchUp()
        final LocalBlockStore other = new LocalBlockStore(tmp.resolve('other'), 'other', false)
        final Path from = ((LocalBlockStore) f.store).blockPath((Cid) run.completion)
        final Path to = other.blockPath((Cid) run.completion)
        Files.createDirectories(to.parent)
        Files.copy(from, to)
        sweepAway([(Cid) run.completion])

        when:
        catchUp(new CompositeStore([f.store, other]))

        then:
        index.isRunIndexed((Cid) run.completion)
    }

    def 'a forget entry is applied once: the overlap re-read changes nothing'() {
        given:
        final Map gone = f.run('gone', [[g: f.raw('g')]])
        catchUp()
        sweepAway([(Cid) gone.completion])
        catchUp()

        when:
        catchUp()

        then:
        index.forgottenRunsSince('lab', null) == 1
    }

    def 'an unreadable Forget block is retried once it can be read'() {
        given:
        final Map gone = f.run('gone', [[g: f.raw('g')]])
        catchUp()
        final Cid forget = sweepAway([(Cid) gone.completion])
        final Path p = ((LocalBlockStore) f.store).blockPath(forget)
        final byte[] bytes = Files.readAllBytes(p)
        Files.delete(p)

        when:
        catchUp()

        then:
        index.isRunIndexed((Cid) gone.completion)

        when:
        f.store.put(forget, new ByteArrayInputStream(bytes), bytes.length)
        catchUp()

        then:
        !index.isRunIndexed((Cid) gone.completion)
    }

    def 'forgottenRunsSince counts only entries newer than the base watermark'() {
        given:
        final Map one = f.run('one', [[a: f.raw('a')]])
        final Map two = f.run('two', [[b: f.raw('b')]])
        catchUp()
        sweepAway([(Cid) one.completion])
        catchUp()
        final String base = index.watermark('lab')
        sweepAway([(Cid) two.completion])
        catchUp()

        expect:
        index.forgottenRunsSince('lab', base) == 1
        index.forgottenRunsSince('lab', null) == 2
    }

    def 'a malformed Forget block is skipped with one warning and not retried'() {
        given: 'a block of kind Forget whose deleted is not a list, logged as forget'
        final Map run = f.run('r', [[x: f.raw('x')]])
        catchUp()
        final Cid bad = f.store.putDagCbor([kind: 'Forget', schema: 1L, asserted_by: 'x', deleted: 'not a list'])
        StoreLog.append(f.store, StoreLogKind.FORGET, bad, ++f.clock)
        final appender = capture(Index)

        when:
        catchUp()
        catchUp()

        then:
        noExceptionThrown()
        index.isRunIndexed((Cid) run.completion)
        index.forgottenRunsSince('lab', null) == 0
        index.metaValue("forget:lab:${bad}".toString())?.endsWith(' 0')
        index.countRows('missing') == 0
        warnings(appender, bad.toString()) == 1
    }

    private static ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> capture(Class type) {
        final logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(type)
        final appender = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>()
        appender.start()
        logger.addAppender(appender)
        return appender
    }

    private static int warnings(ch.qos.logback.core.read.ListAppender appender, String containing) {
        return appender.list.count { e -> e.level == ch.qos.logback.classic.Level.WARN && e.formattedMessage.contains(containing) } as int
    }
}
