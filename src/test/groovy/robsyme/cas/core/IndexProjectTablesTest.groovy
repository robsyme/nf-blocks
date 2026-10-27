package robsyme.cas.core

import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

import spock.lang.Specification
import spock.lang.TempDir

/** P4 plan 1.3-1.5: the project, project_stats, run_input and handoff tables, and the claim_current value index. */
class IndexProjectTablesTest extends Specification {

    @TempDir Path tmp
    Index index

    def setup() {
        index = Index.open(tmp.resolve('cache.sqlite'))
    }

    def cleanup() { index?.close() }

    private static List<List<Object>> rows(Path db, String sql) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            final def rs = c.createStatement().executeQuery(sql)
            final int n = rs.metaData.columnCount
            final List<List<Object>> out = []
            while( rs.next() )
                out << (1..n).collect { int i -> rs.getObject(i) }
            return out
        }
        finally {
            c.close()
        }
    }

    private static void exec(Path db, String sql) {
        final Connection c = DriverManager.getConnection("jdbc:sqlite:${db}")
        try {
            c.createStatement().executeUpdate(sql)
        }
        finally {
            c.close()
        }
    }

    private static Set<String> objects(Path db) {
        return rows(db, "SELECT name FROM sqlite_master WHERE type IN ('table', 'index')").collect { it[0] as String } as Set
    }

    private static final List<String> P4_OBJECTS = ['project', 'project_stats', 'run_input', 'run_input_address',
                                                    'run_input_completion', 'handoff', 'claim_current_attribute_value']

    def 'a new cache has the P4 tables and indexes'() {
        expect:
        objects(index.file).containsAll(P4_OBJECTS)
        rows(index.file, "SELECT sql FROM sqlite_master WHERE name = 'claim_current_attribute_value'") ==
            [['CREATE INDEX claim_current_attribute_value ON claim_current(attribute, value)']]
    }

    def 'reset empties the P4 tables'() {
        given:
        exec(index.file, "INSERT INTO project VALUES ('p', 'n', '', 'a', 't')")
        exec(index.file, "INSERT INTO run_input VALUES ('c', 'task', 'x.fq', 'b', 1, 's', 0)")
        exec(index.file, "INSERT INTO handoff VALUES ('h', 't', 's', NULL, 'a', 't')")

        when:
        index.reset()

        then:
        index.countRows('project') == 0
        index.countRows('run_input') == 0
        index.countRows('handoff') == 0
    }
}
