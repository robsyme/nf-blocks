package robsyme.cas

import java.nio.file.Path

import spock.lang.Specification

/**
 * Behaviour of the `cas` config scope described in DESIGN.md section 2.
 */
class CasConfigTest extends Specification {

    private static Map storeConfig(Map extra = [:]) {
        return [cas: [stores: [lab: [location: '/data/cas']]] + extra]
    }

    def 'reads the writable alias and its location from the lineage location'() {
        when:
        def config = CasConfig.from(storeConfig(), 'cas://lab')

        then:
        config.writableAlias == 'lab'
        config.writableLocation == Path.of('/data/cas')
    }

    def 'defaults asserted_by to anonymous and never to the OS user name'() {
        when:
        def config = CasConfig.from(storeConfig(), 'cas://lab')

        then:
        config.assertedBy == 'anonymous'
        config.assertedBy != System.getProperty('user.name')
    }

    def 'takes asserted_by from the config when set'() {
        when:
        def config = CasConfig.from([cas: [stores: [lab: [location: '/data/cas']], asserted_by: 'gate']], 'cas://lab')

        then:
        config.assertedBy == 'gate'
    }

    def 'defaults the member list to every alias with the writable one first'() {
        given:
        def sessionConfig = [cas: [stores: [
            shared: [location: '/mnt/bundle'],
            lab   : [location: '/data/cas'],
            extra : [location: '/mnt/extra'] ]]]

        when:
        def config = CasConfig.from(sessionConfig, 'cas://lab')

        then:
        config.members.first() == 'lab'
        config.members.toSet() == ['lab', 'shared', 'extra'].toSet()
        config.locationOf('shared') == Path.of('/mnt/bundle')
    }

    def 'honours an explicit resolve list but still puts the writable member first'() {
        given:
        def sessionConfig = [cas: [
            stores : [lab: [location: '/data/cas'], shared: [location: '/mnt/bundle']],
            resolve: ['shared', 'lab'] ]]

        when:
        def config = CasConfig.from(sessionConfig, 'cas://lab')

        then:
        config.members == ['lab', 'shared']
    }

    def 'rejects an explicit resolve entry that is not a configured store'() {
        when:
        CasConfig.from([cas: [stores: [lab: [location: '/data/cas']], resolve: ['lab', 'ghost']]], 'cas://lab')

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('ghost')
    }

    def 'rejects a resolve given as a String rather than a list'() {
        when:
        // 'lab' as List would char-split to ['l','a','b']; refuse it instead.
        CasConfig.from([cas: [stores: [lab: [location: '/data/cas']], resolve: 'lab']], 'cas://lab')

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('cas.resolve must be a list')
    }

    def 'accepts an alias matching the alias pattern'() {
        expect:
        CasConfig.from([cas: [stores: [(alias): [location: '/data/cas']]]], "cas://$alias").writableAlias == alias

        where:
        alias << ['a', 'lab', 'lab-2', 'lab_2', 'l' + ('x' * 31)]
    }

    def 'rejects an alias that does not match the alias pattern'() {
        when:
        CasConfig.from([cas: [stores: [(alias): [location: '/data/cas']]]], "cas://$alias")

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains(alias)

        where:
        alias << ['Lab', '2lab', 'lab.2', '-lab', 'l' + ('x' * 32), '']
    }

    def 'rejects an alias that parses as a content address'() {
        given:
        def cid = 'bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku'

        when:
        CasConfig.from([cas: [stores: [(cid): [location: '/data/cas']]]], "cas://$cid")

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains(cid)
        e.message.toLowerCase().contains('content address')
    }

    def 'names the missing alias when cas.stores has no entry for it'() {
        when:
        CasConfig.from([cas: [stores: [other: [location: '/data/cas']]]], 'cas://lab')

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('lab')
        e.message.contains('cas.stores.lab')
    }

    def 'names the alias when its store entry has no location'() {
        when:
        CasConfig.from([cas: [stores: [lab: [:]]]], 'cas://lab')

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('cas.stores.lab.location')
    }

    def 'rejects a lineage location that is not a cas uri'() {
        when:
        CasConfig.from(storeConfig(), location)

        then:
        thrown(IllegalArgumentException)

        where:
        location << ['file:///x', './.lineage', null, 'cas://', 'cas://lab/nested']
    }

    def 'tolerates a session config with no cas scope by naming the missing scope'() {
        when:
        CasConfig.from([:], 'cas://lab')

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('cas.stores.lab')
    }

    def 'reads the lineage location out of the session config'() {
        given:
        def sessionConfig = [
            lineage: [enabled: true, store: [location: 'cas://lab']],
            cas    : [stores: [lab: [location: '/data/cas']]] ]

        when:
        def config = CasConfig.fromSession(sessionConfig)

        then:
        config.writableAlias == 'lab'
        config.writableLocation == Path.of('/data/cas')
    }

    def 'rejects a session config whose lineage store is not a cas location'() {
        when:
        CasConfig.fromSession(sessionConfig)

        then:
        thrown(IllegalArgumentException)

        where:
        sessionConfig << [ [:], [lineage: [enabled: true]], [lineage: [store: [location: './.lineage']]] ]
    }

    def 'extracts the alias from a cas uri'() {
        expect:
        CasConfig.aliasOf('cas://lab') == 'lab'
        CasConfig.aliasOf('file:///x') == null
        CasConfig.aliasOf(null) == null
    }

    def 'cas.snapshot.maxBytes defaults to 64 MiB and accepts bytes, MemoryUnit and text'() {
        expect:
        CasConfig.from(storeConfig(), 'cas://lab').snapshotMaxBytes == 64L * 1024 * 1024
        CasConfig.from(withSnapshot(1000), 'cas://lab').snapshotMaxBytes == 1000L
        CasConfig.from(withSnapshot(nextflow.util.MemoryUnit.of('2 MB')), 'cas://lab').snapshotMaxBytes == 2L * 1024 * 1024
        CasConfig.from(withSnapshot('3 MB'), 'cas://lab').snapshotMaxBytes == 3L * 1024 * 1024
    }

    def 'a non-positive snapshot cap is refused'() {
        when:
        CasConfig.from(withSnapshot(0), 'cas://lab')

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('cas.snapshot.maxBytes')
    }

    def 'cas.index.path is carried as the index override'() {
        expect:
        CasConfig.from([cas: [stores: [lab: [location: '/data/cas']], index: [path: '/tmp/i.sqlite']]], 'cas://lab').indexOverride == '/tmp/i.sqlite'
        CasConfig.from(storeConfig(), 'cas://lab').indexOverride == null
    }

    private Map withSnapshot(Object maxBytes) {
        return [cas: [stores: [lab: [location: '/data/cas']], snapshot: [maxBytes: maxBytes]]]
    }

    def 'S3 members resolve like local ones, writable first; texts name the cache file'() {
        given:
        final Map cfg = [cas: [stores: [lab: [location: 's3://bucket/cas/'], shared: [location: '/mnt/shared'], priv: [location: 's3://other']]]]

        when:
        final CasConfig config = CasConfig.from(cfg, 'cas://lab')

        then:
        config.members == ['lab', 'shared', 'priv']
        config.isRemote('lab') && config.isRemote('priv') && !config.isRemote('shared')
        config.remoteOf('lab').bucket == 'bucket'
        config.remoteOf('lab').prefix == 'cas/'
        config.remoteOf('priv').prefix == ''
        config.writableLocation == null
        config.locationOf('shared') == Path.of('/mnt/shared')
        config.locationTexts() == ['s3://bucket/cas', '/mnt/shared', 's3://other']
        config.remoteLocationOf('lab') == URI.create('s3://bucket/cas')
    }

    def 's3://bkt/p and s3://bkt/p/ are one member and one cache name (Review Focus 5)'() {
        expect:
        CasConfig.from([cas: [stores: [lab: [location: 's3://bkt/p']]]], 'cas://lab').locationTexts() ==
            CasConfig.from([cas: [stores: [lab: [location: 's3://bkt/p/']]]], 'cas://lab').locationTexts()
    }

    def 'a location in another scheme is refused, naming the store'() {
        when:
        CasConfig.from([cas: [stores: [lab: [location: 'gs://bucket/cas']]]], 'cas://lab')

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('cas.stores.lab.location')
        e.message.contains('gs://bucket/cas')
    }

    def 'archive storage classes are refused for a writable S3 member; infrequent access warns about packing'() {
        when:
        CasConfig.from([aws: [client: [storageClass: cls]], cas: [stores: [lab: [location: 's3://bkt']]]], 'cas://lab')

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains(cls)
        e.message.contains('packing')

        where:
        cls << ['GLACIER', 'DEEP_ARCHIVE']
    }

    def 'the storage-class warning (#cls)'() {
        expect:
        CasConfig.from([aws: [client: [storageClass: cls]], cas: [stores: [lab: [location: 's3://bkt']]]], 'cas://lab')
            .storageClassWarning?.contains(fragment) ?: fragment == null

        where:
        cls                   | fragment
        'STANDARD_IA'         | 'packing'
        'INTELLIGENT_TIERING' | 'packing'
        'GLACIER_IR'          | 'nf-amazon ignores'
        'STANDARD'            | null
    }

    def 'a local writable member ignores the storage class'() {
        expect:
        CasConfig.from([aws: [client: [storageClass: 'GLACIER']], cas: [stores: [lab: [location: '/data/cas']]]], 'cas://lab').storageClassWarning == null
    }

    def 'cas.tmpDir defaults to java.io.tmpdir; cas.nodeHash defaults to fusion.enabled'() {
        expect:
        CasConfig.from(storeConfig(), 'cas://lab').tmpDir == Path.of(System.getProperty('java.io.tmpdir'))
        CasConfig.from([cas: [stores: [lab: [location: '/data/cas']], tmpDir: '/scratch']], 'cas://lab').tmpDir == Path.of('/scratch')
        !CasConfig.nodeHashEnabled([:])
        CasConfig.nodeHashEnabled([fusion: [enabled: true]])
        !CasConfig.nodeHashEnabled([fusion: [enabled: true], cas: [nodeHash: false]])
        CasConfig.nodeHashEnabled([cas: [nodeHash: true]])
    }

    def 'a cas.nodeHash that is not a boolean is refused'() {
        when:
        CasConfig.from([cas: [stores: [lab: [location: '/data/cas']], nodeHash: 'yes']], 'cas://lab')

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('cas.nodeHash')
    }

    def 'an S3 location with a query string is refused, since it would parse differently than it validated'() {
        when:
        CasConfig.from([cas: [stores: [lab: [location: '/data/cas'], priv: [location: 's3://bucket/a?b']]]], 'cas://lab')

        then:
        final IllegalArgumentException e = thrown()
        e.message.contains('priv')
        e.message.contains('is not an S3 URI')
    }
}
