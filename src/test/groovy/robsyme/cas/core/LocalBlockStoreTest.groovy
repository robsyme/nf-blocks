package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors

import spock.lang.Specification
import spock.lang.TempDir

/** DESIGN.md §5: the on-disk layout and the write protocol. */
class LocalBlockStoreTest extends Specification {

    @TempDir
    Path root

    LocalBlockStore store

    def setup() {
        store = new LocalBlockStore(root, 'lab', true)
    }

    private static InputStream stream(String text) {
        new ByteArrayInputStream(text.getBytes('UTF-8'))
    }

    private List<Path> tempFiles() {
        if( !Files.exists(root) ) return []
        Files.walk(root).filter { Path p -> p.fileName.toString().startsWith('.tmp-') }.collect(Collectors.toList())
    }

    def 'the store reports its alias and writability'() {
        expect:
        store.alias() == 'lab'
        store.isWritable()
        !new LocalBlockStore(root, 'shared', false).isWritable()
    }

    def 'putStreaming places the block under the last two characters of its cid'() {
        when:
        def cid = store.putStreaming(stream('hello\n'))

        then:
        cid.toString() == 'bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am'

        and:
        def block = root.resolve("blocks/${cid.toString()[-2..-1]}/${cid}")
        Files.exists(block)
        Files.size(block) == 6
        Files.readAllBytes(block) == 'hello\n'.getBytes('UTF-8')

        and: 'blocks are read-only for everybody'
        Files.getPosixFilePermissions(block) == PosixFilePermissions.fromString('r--r--r--')

        and: 'no temporary file survives'
        tempFiles().isEmpty()
    }

    def 'storing identical bytes twice is a no-op that returns the same address'() {
        given:
        def first = store.putStreaming(stream('hello\n'))
        def block = root.resolve("blocks/${first.toString()[-2..-1]}/${first}")
        def stamp = Files.getLastModifiedTime(block)
        def modified = store.lastModifiedMillis(first)

        when:
        def second = store.putStreaming(stream('hello\n'))

        then:
        second == first
        Files.getLastModifiedTime(block) == stamp
        store.lastModifiedMillis(first) == modified
        tempFiles().isEmpty()
    }

    def 'has, size, open and lastModifiedMillis answer for a stored block'() {
        given:
        def cid = store.putStreaming(stream('hello\n'))

        expect:
        store.has(cid)
        store.size(cid) == 6
        store.open(cid).withStream { it.text } == 'hello\n'
        store.lastModifiedMillis(cid) == store.lastModifiedMillis(cid)
        store.lastModifiedMillis(cid) > 0
    }

    def 'an absent block is reported as such by #method'() {
        given:
        def absent = Cid.parse('bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')

        expect:
        !store.has(absent)

        when:
        switch( method ) {
            case 'size': store.size(absent); break
            case 'open': store.open(absent); break
            case 'lastModifiedMillis': store.lastModifiedMillis(absent); break
        }

        then:
        def e = thrown(NoSuchBlockException)
        e.message.contains(absent.toString())
        e.cid == absent

        where:
        method << ['size', 'open', 'lastModifiedMillis']
    }

    def 'put stores bytes that are already known to hash to the given cid'() {
        given:
        def cid = Cid.parse('bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am')

        when:
        store.put(cid, stream('hello\n'), 6L)

        then:
        store.has(cid)
        store.size(cid) == 6

        and: 'a second put of the same address is a no-op'
        store.put(cid, stream('hello\n'), 6L)
        store.has(cid)
        tempFiles().isEmpty()
    }

    def 'put refuses bytes that do not hash to the cid and leaves nothing behind'() {
        given:
        def cid = Cid.parse('bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am')

        when:
        store.put(cid, stream('goodbye\n'), 8L)

        then:
        thrown(IOException)
        !store.has(cid)
        tempFiles().isEmpty()
        store.listBlocks().withCloseable { it.count() } == 0L
    }

    def 'put refuses bytes whose length is not the announced size'() {
        given:
        def cid = Cid.parse('bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am')

        when:
        store.put(cid, stream('hello\n'), 7L)

        then:
        thrown(IOException)
        !store.has(cid)
        tempFiles().isEmpty()
    }

    def 'listBlocks enumerates every block'() {
        given:
        def cids = (1..5).collect { store.putStreaming(stream("block $it\n")) }
        cids << store.putDagCbor(['kind': 'Test'])

        when:
        def listed = store.listBlocks().withCloseable { it.collect(Collectors.toList()) }

        then:
        listed.toSet() == cids.toSet()
        listed.size() == 6
    }

    def 'listBlocks of an empty store is empty'() {
        expect:
        store.listBlocks().withCloseable { it.count() } == 0L
    }

    def 'putDagCbor encodes, hashes and stores a metadata block'() {
        given:
        def value = ['kind': 'DirectoryManifest', 'schema': 1L, 'entries': []]

        when:
        def cid = store.putDagCbor(value)

        then:
        cid.isDagCbor()
        cid.toString().startsWith('bafy')
        cid == DagCbor.cidOf(DagCbor.encode(value))

        and: 'the block decodes back to what was stored'
        DagCbor.decode(store.open(cid).withCloseable { it.bytes }) == value

        and:
        store.putDagCbor([:]).toString() == 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'
    }

    def 'a read-only store refuses every write'() {
        given:
        def readOnly = new LocalBlockStore(root, 'shared', false)
        def cid = Cid.parse('bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am')

        when:
        switch( method ) {
            case 'put': readOnly.put(cid, stream('hello\n'), 6L); break
            case 'putStreaming': readOnly.putStreaming(stream('hello\n')); break
            case 'putDagCbor': readOnly.putDagCbor([:]); break
        }

        then:
        def e = thrown(IllegalStateException)
        e.message.contains('shared')

        and: 'nothing was written'
        readOnly.listBlocks().withCloseable { it.count() } == 0L

        where:
        method << ['put', 'putStreaming', 'putDagCbor']
    }

    def 'sixteen threads storing the same bytes leave exactly one block'() {
        given:
        def payload = new byte[3 * 1024 * 1024]
        new Random(11).nextBytes(payload)
        def threads = 16
        def start = new CountDownLatch(1)
        def done = new CountDownLatch(threads)
        def results = Collections.synchronizedList(new ArrayList<Cid>())
        def failures = new AtomicInteger()

        when:
        (1..threads).each {
            Thread.start {
                try {
                    start.await()
                    results << store.putStreaming(new ByteArrayInputStream(payload))
                }
                catch( Throwable t ) {
                    failures.incrementAndGet()
                    t.printStackTrace()
                }
                finally {
                    done.countDown()
                }
            }
        }
        start.countDown()

        then:
        done.await(60, TimeUnit.SECONDS)
        failures.get() == 0
        results.size() == threads
        results.toSet().size() == 1

        and: 'one block, no leftovers'
        store.listBlocks().withCloseable { it.count() } == 1L
        tempFiles().isEmpty()
        store.size(results[0]) == payload.length
    }
}
