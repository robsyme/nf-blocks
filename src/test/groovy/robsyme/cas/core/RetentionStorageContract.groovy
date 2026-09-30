package robsyme.cas.core

import spock.lang.Specification

/** One contract for every RetentionStorage (as CoordinateTreeContract is for coordinate trees). */
abstract class RetentionStorageContract extends Specification {

    /** A fresh, empty member. */
    abstract RetentionStorage storage()
    /** Writes a block into that member through its own BlockStore, returning its cid. */
    abstract Cid writeBlock(byte[] bytes)
    /** Leaves one scratch object in that member. */
    abstract void leaveScratch()
    /** Writes one Store Log entry by name. */
    abstract void writeLogEntry(String name)
    abstract boolean logEntryExists(String name)

    def 'a lock is created once, replaced only at its version, and read back'() {
        given:
        final RetentionStorage s = storage()

        expect:
        s.readLock() == null

        when:
        final String v1 = s.createLock('one'.bytes)

        then:
        v1 != null
        s.createLock('two'.bytes) == null
        new String(s.readLock().body) == 'one'
        s.readLock().version == v1

        when:
        final String v2 = s.replaceLock(v1, 'three'.bytes)

        then:
        v2 != null && v2 != v1
        s.replaceLock(v1, 'four'.bytes) == null
        new String(s.readLock().body) == 'three'
        s.readLock().lastModifiedMillis > 0
    }

    def 'registrations are written, rewritten, listed and deleted'() {
        given:
        final RetentionStorage s = storage()

        when:
        s.putLive('s1', '{"session":"s1"}'.bytes)
        s.putLive('s2', '{}'.bytes)
        s.putLive('s1', '{"session":"s1"}'.bytes)

        then:
        s.listLive()*.name.sort() == ['s1', 's2']
        new String(s.readLive('s1')) == '{"session":"s1"}'
        s.readLive('absent') == null
        s.listLive().every { it.lastModifiedMillis > 0 }

        when:
        s.deleteLive('s1')
        s.deleteLive('absent')

        then:
        s.listLive()*.name == ['s2']
    }

    def 'ledgers are listed in name order, read, replaced and deleted'() {
        given:
        final RetentionStorage s = storage()

        when:
        s.writeLedger('0000000000002-b', 'b'.bytes)
        s.writeLedger('0000000000001-a', 'a'.bytes)
        s.writeLedger('0000000000002-b', 'b2'.bytes)

        then:
        s.listLedgers() == ['0000000000001-a', '0000000000002-b']
        new String(s.readLedger('0000000000002-b')) == 'b2'
        s.readLedger('absent') == null

        when:
        s.deleteLedger('0000000000001-a')

        then:
        s.listLedgers() == ['0000000000002-b']
    }

    def 'blocks are listed with sizes and ages, and deleted'() {
        given:
        final RetentionStorage s = storage()
        final Cid a = writeBlock('alpha'.bytes)
        final Cid b = writeBlock('bravo!'.bytes)

        expect:
        s.listBlockStats().collectEntries { [(it.cid): it.size] } == [(a): 5L, (b): 6L]
        s.listBlockStats().every { it.lastModifiedMillis > 0 }

        when:
        final List<Cid> failed = s.deleteBlocks([a, Cid.parse('bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')])

        then:
        failed == []
        s.listBlockStats()*.cid == [b]
    }

    def 'scratch is listed and deleted, and a log entry is deleted by name'() {
        given:
        final RetentionStorage s = storage()
        leaveScratch()
        writeLogEntry('0000000000001-claim-bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')

        when:
        final List<Stamped> scratch = s.listScratch()

        then:
        scratch.size() == 1

        when:
        s.deleteScratch(scratch[0])
        s.deleteLogEntry('0000000000001-claim-bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')
        s.deleteLogEntry('absent')

        then:
        s.listScratch() == []
        !logEntryExists('0000000000001-claim-bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku')
    }

    def 'the store clock is a real time'() {
        given:
        final RetentionStorage s = storage()
        s.listLive()

        expect:
        s.nowMillis() > 1_700_000_000_000L
    }
}
