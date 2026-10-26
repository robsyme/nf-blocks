package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import spock.lang.Specification
import spock.lang.TempDir

/**
 * Index.open when another opener is at the same file (P3 final review item
 * 4): the explore server opens one cache for each request, so two dry runs
 * and a copy can open it at once. No opener may delete a database another
 * is creating or holds.
 */
class IndexOpenRaceTest extends Specification {

    @TempDir Path tmp

    private static String meta(Path db, String key) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            final ResultSet rs = c.createStatement().executeQuery("SELECT value FROM meta WHERE key = '${key}'")
            return rs.next() ? rs.getString(1) : null
        }
        finally {
            c.close()
        }
    }

    def 'two openers of a new cache at once get the same, whole database, and neither deletes it under the other'() {
        expect:
        (1..20).every { int round ->
            final Path db = tmp.resolve("round-${round}/index.sqlite")
            final CountDownLatch go = new CountDownLatch(1)
            final List<Object> opened = Collections.synchronizedList([])
            final List<Thread> threads = (1..2).collect {
                Thread.start {
                    go.await()
                    try {
                        opened << Index.open(db)
                    }
                    catch( Throwable t ) {
                        opened << t
                    }
                }
            }
            go.countDown()
            threads*.join(20_000)
            assert opened.size() == 2 && opened.every { it instanceof Index }: "round ${round}: ${opened}"
            final Index a = (Index) opened[0]
            final Index b = (Index) opened[1]
            a.setMetaValue('from', 'a')
            b.setMetaValue('also', 'b')
            a.close()
            b.close()
            assert meta(db, 'from') == 'a' && meta(db, 'also') == 'b': "round ${round}: a write went to a deleted file"
            true
        }
    }

    def 'a cache whose schema another connection has not yet committed is waited for, never recreated'() {
        given: 'another opener has created the file and holds its schema in an open transaction'
        Path db = tmp.resolve('index.sqlite')
        final Connection creator = DriverManager.getConnection("jdbc:sqlite:${db}")
        creator.createStatement().withCloseable { it.execute('PRAGMA journal_mode=WAL') }
        creator.autoCommit = false
        final Statement s = creator.createStatement()
        for( String sql : Index.ddl() )
            s.executeUpdate(sql)
        s.executeUpdate("INSERT INTO schema_version(version) VALUES (${Index.SCHEMA_VERSION})")
        s.executeUpdate("INSERT INTO meta(key, value) VALUES ('creator', 'first')")
        s.close()

        when: 'a second opener arrives, and the first commits a moment later'
        Index second = null
        Throwable failed = null
        final Thread opener = Thread.start {
            try {
                second = Index.open(db)
            }
            catch( Throwable t ) {
                failed = t
            }
        }
        Thread.sleep(1000)
        creator.commit()
        creator.close()
        opener.join(20_000)

        then: 'the second opener uses the first one\'s database'
        failed == null
        second != null
        meta(db, 'creator') == 'first'

        cleanup:
        second?.close()
    }

    def 'a stale cache held by another writer past the busy timeout is not deleted; the open fails instead'() {
        given: 'a cache of an older schema version that another connection is writing'
        Path db = tmp.resolve('index.sqlite')
        final Connection holder = DriverManager.getConnection("jdbc:sqlite:${db}")
        holder.createStatement().withCloseable { it.execute('PRAGMA journal_mode=WAL') }
        holder.createStatement().executeUpdate('CREATE TABLE schema_version(version INTEGER NOT NULL)')
        holder.createStatement().executeUpdate('INSERT INTO schema_version(version) VALUES (3)')
        holder.createStatement().executeUpdate('CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)')
        holder.autoCommit = false
        holder.createStatement().executeUpdate("INSERT INTO meta(key, value) VALUES ('holder', 'still here')")

        when:
        Index.open(db)

        then:
        final IllegalStateException e = thrown()
        e.message.contains('busy')
        Files.exists(db)

        when: 'the holder commits into the same file'
        holder.commit()
        holder.close()

        then:
        meta(db, 'holder') == 'still here'
    }
}
