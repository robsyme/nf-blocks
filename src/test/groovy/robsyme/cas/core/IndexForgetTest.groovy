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

    /** The composition a forget entry asks: counts `has` calls, and throws for the cids in `failing`. */
    static class Holders implements BlockStore {
        final BlockStore delegate
        final Set<Cid> failing
        final List<Cid> asked = []

        Holders(BlockStore delegate, Set<Cid> failing = [] as Set) { this.delegate = delegate; this.failing = failing }

        @Override String alias() { delegate.alias() }
        @Override boolean has(Cid cid) {
            asked << cid
            if( failing.contains(cid) )
                throw new RuntimeException('Access Denied (Service: S3, Status Code: 403)')
            return delegate.has(cid)
        }
        @Override long size(Cid cid) { delegate.size(cid) }
        @Override InputStream open(Cid cid) { delegate.open(cid) }
        @Override long lastModifiedMillis(Cid cid) { delegate.lastModifiedMillis(cid) }
        @Override void put(Cid cid, InputStream input, long expectedSize) { delegate.put(cid, input, expectedSize) }
        @Override Cid putStreaming(InputStream input) { delegate.putStreaming(input) }
        @Override Cid putDagCbor(Object value) { delegate.putDagCbor(value) }
        @Override java.util.stream.Stream<Cid> listBlocks() { delegate.listBlocks() }
        @Override boolean isWritable() { delegate.isWritable() }
    }

    def 'a holder lookup that throws keeps that cid\'s rows; the rest are forgotten and the watermark advances'() {
        given:
        final Map denied = f.run('denied', [[d: f.raw('d')]])
        final Map gone = f.run('gone', [[g: f.raw('g')]])
        catchUp()
        final Cid forget = sweepAway([(Cid) denied.completion, (Cid) gone.completion])
        final Holders holders = new Holders(f.store, [(Cid) denied.completion] as Set)
        final appender = capture(Index)

        when:
        catchUp(holders)

        then:
        noExceptionThrown()
        index.isRunIndexed((Cid) denied.completion)
        !index.isRunIndexed((Cid) gone.completion)
        index.watermark('lab') == StoreLog.of(f.store).read()[0].name
        index.metaValue("forget:lab:${forget}".toString()) != null
        warnings(appender, forget.toString()) == 1
        appender.list.find { it.formattedMessage.contains(forget.toString()) }.formattedMessage.contains('1 listed')
    }

    def 'a forget entry listing only cids this cache never indexed asks the composition nothing'() {
        given:
        f.run('r', [[x: f.raw('x')]])
        catchUp()
        final Map other = new RetentionFixture(tmp.resolve('elsewhere')).run('o', [[y: f.raw('y')]])
        sweepAway([(Cid) other.completion, (Cid) other.collection] + (List<Cid>) other.items)
        final Holders holders = new Holders(f.store)

        when:
        catchUp(holders)

        then:
        holders.asked == []
        index.countRows('run') == 1
    }

    def 'a Forget block that is present but not a Forget is applied with 0 runs, warned once, and not retried'() {
        given: 'a forget entry naming a block of another kind'
        final Map run = f.run('present-not-forget', [[pnf: f.raw('pnf')]])
        catchUp()
        StoreLog.append(f.store, StoreLogKind.FORGET, (Cid) run.collection, ++f.clock)
        final appender = capture(Index)

        when:
        catchUp()
        catchUp()

        then:
        index.isRunIndexed((Cid) run.completion)
        index.metaValue("forget:lab:${run.collection}".toString())?.endsWith(' 0')
        index.countRows('missing') == 0
        warnings(appender, "forget ${run.collection}".toString()) == 1
    }

    def 'a Forget block that is absent stays missing and is retried'() {
        given:
        f.run('r', [[x: f.raw('x')]])
        catchUp()
        final Cid absent = DagCbor.cidOf(DagCbor.encode(new Forget('never written', [(Cid) f.raw('x')]).toCbor()))
        StoreLog.append(f.store, StoreLogKind.FORGET, absent, ++f.clock)

        when:
        catchUp()
        catchUp()

        then:
        index.metaValue("forget:lab:${absent}".toString()) == null
        index.countRows('missing') == 1
    }

    def 'a Forget block that is present but cannot be read for now is retried, not recorded as applied'() {
        given: 'a store whose read of the Forget block fails once with an IO error'
        final Map gone = f.run('io-retry', [[ir: f.raw('ir')]])
        catchUp()
        final Cid forget = sweepAway([(Cid) gone.completion])
        int failures = 1
        final BlockStore flaky = new LocalBlockStore(tmp.resolve('lab'), 'lab', true) {
            @Override InputStream open(Cid cid) {
                if( cid == forget && failures-- > 0 )
                    throw new IOException('connection reset')
                return super.open(cid)
            }
        }

        when:
        index.catchUp(flaky, StoreLog.of(flaky), 'lab', null, tmp, Collections.<String> emptySet(), flaky)

        then:
        index.isRunIndexed((Cid) gone.completion)
        index.metaValue("forget:lab:${forget}".toString()) == null

        when:
        index.catchUp(flaky, StoreLog.of(flaky), 'lab', null, tmp, Collections.<String> emptySet(), flaky)

        then:
        !index.isRunIndexed((Cid) gone.completion)
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
