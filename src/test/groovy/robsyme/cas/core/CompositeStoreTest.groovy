package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors
import java.util.stream.Stream

import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §5: reads resolve in order, writes go to member 0. */
class CompositeStoreTest extends Specification {

    @TempDir
    Path tempDir

    /** Counts what the composite asked of each member. */
    static class Recording implements BlockStore {
        final LocalBlockStore delegate
        int opens = 0
        int hasChecks = 0

        Recording(LocalBlockStore delegate) { this.delegate = delegate }

        @Override String alias() { delegate.alias() }
        @Override boolean has(Cid cid) { hasChecks++; delegate.has(cid) }
        @Override long size(Cid cid) { delegate.size(cid) }
        @Override InputStream open(Cid cid) { opens++; delegate.open(cid) }
        @Override long lastModifiedMillis(Cid cid) { delegate.lastModifiedMillis(cid) }
        @Override void put(Cid cid, InputStream input, long expectedSize) { delegate.put(cid, input, expectedSize) }
        @Override Cid putStreaming(InputStream input) { delegate.putStreaming(input) }
        @Override Cid putDagCbor(Object value) { delegate.putDagCbor(value) }
        @Override Stream<Cid> listBlocks() { delegate.listBlocks() }
        @Override boolean isWritable() { delegate.isWritable() }
    }

    Recording writable
    Recording readOnly
    CompositeStore store

    def setup() {
        writable = new Recording(new LocalBlockStore(Files.createDirectories(tempDir.resolve('lab')), 'lab', true))
        readOnly = new Recording(new LocalBlockStore(Files.createDirectories(tempDir.resolve('bundle')), 'bundle', false))
        store = new CompositeStore([writable, readOnly])
    }

    private static InputStream stream(String text) {
        new ByteArrayInputStream(text.getBytes('UTF-8'))
    }

    def 'the composite takes its identity from the writable member'() {
        expect:
        store.alias() == 'lab'
        store.isWritable()
    }

    def 'construction requires at least one member'() {
        when:
        new CompositeStore([])

        then:
        thrown(IllegalArgumentException)
    }

    def 'construction requires a writable member 0'() {
        when:
        new CompositeStore([readOnly, writable])

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('bundle')
    }

    def 'a read resolves in member order and stops at the first hit'() {
        given: 'both members hold the same block'
        def cid = writable.delegate.putStreaming(stream('hello\n'))
        def sameBytes = new LocalBlockStore(readOnly.delegate.root, 'bundle', true)
        sameBytes.putStreaming(stream('hello\n'))
        writable.opens = 0
        readOnly.opens = 0

        when:
        def text = store.open(cid).withStream { it.text }

        then:
        text == 'hello\n'
        writable.opens == 1
        readOnly.opens == 0

        and:
        store.size(cid) == 6
        store.has(cid)
        store.lastModifiedMillis(cid) == writable.delegate.lastModifiedMillis(cid)
    }

    def 'a read falls through to a later member'() {
        given: 'only the read-only member holds the block'
        def sameBytes = new LocalBlockStore(readOnly.delegate.root, 'bundle', true)
        def cid = sameBytes.putStreaming(stream('shared\n'))

        expect:
        store.has(cid)
        store.size(cid) == 7
        store.open(cid).withStream { it.text } == 'shared\n'
        store.lastModifiedMillis(cid) == readOnly.delegate.lastModifiedMillis(cid)
        !writable.delegate.has(cid)
    }

    def 'a block held by nobody is a NoSuchBlockException'() {
        given:
        def absent = Cid.parse('bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')

        expect:
        !store.has(absent)

        when:
        store.open(absent)

        then:
        thrown(NoSuchBlockException)

        when:
        store.size(absent)

        then:
        thrown(NoSuchBlockException)
    }

    def 'a write goes to member 0 even when a later member already holds the block'() {
        given:
        def sameBytes = new LocalBlockStore(readOnly.delegate.root, 'bundle', true)
        def cid = sameBytes.putStreaming(stream('hello\n'))

        when:
        def written = store.putStreaming(stream('hello\n'))

        then:
        written == cid
        writable.delegate.has(cid)

        and: 'the same holds for the addressed and the metadata writes'
        store.put(cid, stream('hello\n'), 6L)
        store.putDagCbor(['kind': 'Test']) == writable.delegate.putDagCbor(['kind': 'Test'])
    }

    def 'listBlocks unions the members without duplicates'() {
        given:
        def shared = writable.delegate.putStreaming(stream('hello\n'))
        def onlyLab = writable.delegate.putStreaming(stream('lab\n'))
        def bundle = new LocalBlockStore(readOnly.delegate.root, 'bundle', true)
        bundle.putStreaming(stream('hello\n'))
        def onlyBundle = bundle.putStreaming(stream('bundle\n'))

        when:
        def listed = store.listBlocks().withCloseable { it.collect(Collectors.toList()) }

        then:
        listed.toSet() == [shared, onlyLab, onlyBundle].toSet()
        listed.size() == 3
    }
}
