package robsyme.cas.core

import java.security.MessageDigest

import spock.lang.Specification

/**
 * DESIGN.md §3: CIDv1 over a sha2-256 multihash, base32 lower, 59 characters.
 * The vectors below are the ones in the contract, recomputed here from
 * sha256 + the multiformats CIDv1 layout.
 */
class CidTest extends Specification {

    private static byte[] sha256(byte[] data) {
        MessageDigest.getInstance('SHA-256').digest(data)
    }

    private static String hex(byte[] data) {
        StringBuilder sb = new StringBuilder()
        for( byte b : data ) sb.append(String.format('%02x', b))
        sb.toString()
    }

    def 'raw cid of the empty input matches the known vector'() {
        when:
        def cid = Cid.of(Cid.RAW, sha256(new byte[0]))

        then:
        cid.toString() == 'bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku'
        cid.toString().length() == 59
        cid.isRaw()
        !cid.isDagCbor()
        cid.codec == 0x55
    }

    def 'raw cid of "hello\\n" matches the known digest and vector'() {
        given:
        def digest = sha256('hello\n'.getBytes('UTF-8'))

        expect:
        hex(digest) == '5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03'
        Cid.of(Cid.RAW, digest).toString() == 'bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am'
    }

    def 'dag-cbor cid of the empty map matches the known vector'() {
        given:
        def digest = sha256([0xa0] as byte[])

        expect:
        hex(digest) == 'c19a797fa1fd590cd2e5b42d1cf5f246e29b91684e2f87404b81dc345c7a56a0'
        def cid = Cid.of(Cid.DAG_CBOR, digest)
        cid.toString() == 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua'
        cid.isDagCbor()
        !cid.isRaw()
        cid.codec == 0x71
    }

    def 'binary form is version, codec and multihash without the multibase prefix'() {
        given:
        def digest = sha256(new byte[0])

        when:
        def bytes = Cid.of(Cid.RAW, digest).bytes()

        then:
        bytes.length == 36
        hex(bytes) == '01551220' + hex(digest)
    }

    def 'parse round-trips every text form'() {
        expect:
        Cid.parse(text).toString() == text

        where:
        text << ['bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku',
                 'bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am',
                 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua']
    }

    def 'parse recovers the codec and the digest'() {
        given:
        def digest = sha256('hello\n'.getBytes('UTF-8'))

        when:
        def cid = Cid.parse('bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am')

        then:
        cid.codec == Cid.RAW
        hex(cid.digest) == hex(digest)
        cid == Cid.of(Cid.RAW, digest)
    }

    def 'parse rejects #why'() {
        when:
        Cid.parse(text)

        then:
        thrown(IllegalArgumentException)

        where:
        why                              | text
        'a null string'                  | null
        'an empty string'                | ''
        'a CIDv0'                        | 'QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG'
        'a multibase other than base32'  | 'zafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku'
        'a character outside base32'     | 'bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyk8'
        'non-zero padding bits'          | 'bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvykb'
        'a truncated string'             | 'bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyk'
        'a trailing character'           | 'bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvykua'
        'a multihash code other than 12' | 'bafkrgihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku'
        'a digest length other than 32'  | 'bafkrehdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvy'
        'a cid version other than 1'     | 'bajkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku'
    }

    def 'of rejects a digest that is not 32 bytes'() {
        when:
        Cid.of(Cid.RAW, new byte[31])

        then:
        thrown(IllegalArgumentException)
    }

    def 'only the two codecs this store addresses are accepted'() {
        when: 'dag-pb, a codec nf-blocks never writes'
        Cid.of(0x70, sha256(new byte[0]))

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('112')      // 0x70 in decimal

        when: 'and the same codec arriving as text'
        Cid.parse('bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')

        then:
        thrown(IllegalArgumentException)

        and:
        !Cid.isCid('bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')
        Cid.isCid('bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')
    }

    def 'equality and hashing are by content'() {
        given:
        def a = Cid.of(Cid.RAW, sha256(new byte[0]))
        def b = Cid.parse('bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')
        def other = Cid.of(Cid.DAG_CBOR, sha256(new byte[0]))

        expect:
        a == b
        a.hashCode() == b.hashCode()
        a != other
        [a, b].toSet().size() == 1
    }

    def 'cids order by their text form'() {
        given:
        def raw = Cid.parse('bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')
        def hello = Cid.parse('bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am')
        def dag = Cid.parse('bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua')

        when:
        def sorted = [dag, raw, hello].sort(false)

        then:
        sorted*.toString() == [hello, raw, dag]*.toString()
        hello < raw
        raw < dag
    }
}
