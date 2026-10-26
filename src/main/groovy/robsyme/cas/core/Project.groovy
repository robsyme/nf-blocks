package robsyme.cas.core

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * A project's root record (explorer v0.3 P4 plan 1.1, DESIGN.md §22). Small
 * and fixed: its current name, its members and its policies are Claims, so
 * the block's address stays the project's identity while those change.
 */
@CompileStatic
@EqualsAndHashCode
@ToString(includePackage = false, includeNames = true)
class Project {

    static final int MAX_NAME_CHARS = 256
    static final int MAX_DESCRIPTION_CHARS = 4096

    final String assertedBy
    /** The name at creation; a `set name` Claim renames. */
    final String name
    /** May be empty. */
    final String description
    /** ISO-8601 UTC with milliseconds, from the request. */
    final String createdAt

    Project(String assertedBy, String name, String description, String createdAt) {
        if( !assertedBy )
            throw new IllegalArgumentException('a Project needs an asserted_by')
        if( name == null || !name.trim() || name.length() > MAX_NAME_CHARS )
            throw new IllegalArgumentException("a Project's name is a non-blank string of at most ${MAX_NAME_CHARS} characters")
        if( description == null || description.length() > MAX_DESCRIPTION_CHARS )
            throw new IllegalArgumentException("a Project's description is a string of at most ${MAX_DESCRIPTION_CHARS} characters")
        if( !createdAt )
            throw new IllegalArgumentException('a Project needs a created_at')
        this.assertedBy = assertedBy
        this.name = name
        this.description = description
        this.createdAt = createdAt
    }

    Map<String, Object> toCbor() {
        final Map<String, Object> map = Records.head(Records.PROJECT)
        map.put('asserted_by', assertedBy)
        map.put('name', name)
        map.put('description', description)
        map.put('created_at', createdAt)
        return map
    }

    static Project fromCbor(Map block) {
        Records.expectKind(block, Records.PROJECT)
        Records.schemaOf(block, [Records.SCHEMA])
        return new Project(
            Records.string(Records.require(block, 'asserted_by'), 'asserted_by'),
            Records.string(Records.require(block, 'name'), 'name'),
            Records.string(Records.require(block, 'description'), 'description'),
            Records.string(Records.require(block, 'created_at'), 'created_at'))
    }
}
