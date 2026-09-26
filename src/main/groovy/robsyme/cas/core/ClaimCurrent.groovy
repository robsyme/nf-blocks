package robsyme.cas.core

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

import groovy.transform.CompileStatic

/**
 * claim_current for one subject, from ClaimState over the Claims a database
 * holds for it. Shared by the cache index and the Index Snapshot, which must
 * recompute from the member's own Claims alone (block explorer spec section 4),
 * so both write the same rows by the same rules.
 */
@CompileStatic
final class ClaimCurrent {

    private ClaimCurrent() {}

    static ClaimState load(Connection c, String subject) {
        final Map<String, ClaimState.Row> rows = new LinkedHashMap<String, ClaimState.Row>()
        final PreparedStatement claims = c.prepareStatement('SELECT claim_cid, verb, attribute, value FROM claim WHERE subject_cid = ?')
        try {
            claims.setString(1, subject)
            final ResultSet rs = claims.executeQuery()
            while( rs.next() )
                rows.put(rs.getString(1), new ClaimState.Row(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), new ArrayList<String>()))
        }
        finally {
            claims.close()
        }
        final PreparedStatement supersedes = c.prepareStatement(
            'SELECT s.claim_cid, s.superseded_cid FROM claim_supersedes s JOIN claim k ON k.claim_cid = s.claim_cid WHERE k.subject_cid = ?')
        try {
            supersedes.setString(1, subject)
            final ResultSet rs = supersedes.executeQuery()
            while( rs.next() )
                rows.get(rs.getString(1))?.supersedes?.add(rs.getString(2))
        }
        finally {
            supersedes.close()
        }
        return ClaimState.of(rows.values())
    }

    /** Replaces the subject's claim_current rows. The caller owns the transaction. */
    static void rewrite(Connection c, String subject) {
        final ClaimState state = load(c, subject)
        final PreparedStatement delete = c.prepareStatement('DELETE FROM claim_current WHERE subject_cid = ?')
        try {
            delete.setString(1, subject)
            delete.executeUpdate()
        }
        finally {
            delete.close()
        }
        final PreparedStatement insert = c.prepareStatement(
            'INSERT INTO claim_current(subject_cid, attribute, value, claim_cid, conflicted) VALUES (?, ?, ?, ?, ?)')
        try {
            for( Map<String, Object> row : state.currentRows() ) {
                insert.setString(1, subject)
                insert.setObject(2, row.attribute)
                insert.setObject(3, row.value)
                insert.setString(4, (String) row.cid)
                insert.setInt(5, (Boolean) row.conflicted ? 1 : 0)
                insert.executeUpdate()
            }
        }
        finally {
            insert.close()
        }
    }
}
