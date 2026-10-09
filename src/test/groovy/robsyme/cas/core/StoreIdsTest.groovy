package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Callable
import java.util.concurrent.Executors

import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §20 decisions 1 and 2. */
class StoreIdsTest extends Specification {

    @TempDir Path tmp

    def 'a new id is 32 lowercase hex characters, and two are different'() {
        expect:
        StoreIds.newId() ==~ /[0-9a-f]{32}/
        StoreIds.newId() != StoreIds.newId()
    }

    def 'absent, the id reads null; ensure creates it once and reads it back'() {
        given:
        final LocalBlockStore store = new LocalBlockStore(tmp.resolve('lab'), 'lab', true)

        expect:
        store.storeIdStorage().read() == null

        when:
        final String id = StoreIds.ensure(store.storeIdStorage())

        then:
        id ==~ /[0-9a-f]{32}/
        Files.readString(tmp.resolve('lab/store-id')) == id + '\n'
        StoreIds.ensure(store.storeIdStorage()) == id
        StoreIds.read(store) == id
    }

    def 'create never rewrites: the second id is refused and the first returned'() {
        given:
        final LocalStoreIdStorage s = new LocalStoreIdStorage(tmp.resolve('lab'))
        final String first = s.create('a' * 32)

        expect:
        first == 'a' * 32
        s.create('b' * 32) == 'a' * 32
        s.read() == 'a' * 32
    }

    def 'two writers racing to create store-id agree on one id'() {
        given:
        final def pool = Executors.newFixedThreadPool(8)
        final List<Callable<String>> calls = (1..8).collect { int i ->
            ({ -> new LocalStoreIdStorage(tmp.resolve('lab')).create(StoreIds.newId()) } as Callable<String>)
        }

        when:
        final Set<String> seen = pool.invokeAll(calls)*.get() as Set

        then:
        seen.size() == 1
        new LocalStoreIdStorage(tmp.resolve('lab')).read() == seen[0]

        cleanup:
        pool.shutdownNow()
    }

    def 'a malformed store-id reads as null and leaves no temp file behind'() {
        given:
        Files.createDirectories(tmp.resolve('lab'))
        Files.writeString(tmp.resolve('lab/store-id'), 'not an id\n')

        expect:
        new LocalStoreIdStorage(tmp.resolve('lab')).read() == null
        Files.list(tmp.resolve('lab')).withCloseable { it.toList() }*.fileName*.toString() == ['store-id']
    }

    def 'a store that is not identified has no id'() {
        expect:
        StoreIds.read(Mock(BlockStore)) == null
    }
}
