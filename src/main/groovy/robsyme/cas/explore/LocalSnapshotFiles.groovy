package robsyme.cas.explore

import java.nio.file.Path

import groovy.transform.CompileStatic
import robsyme.cas.core.IndexSnapshot

/**
 * A member's files for the explorer when background indexing is on: the
 * member's Index Snapshot URL answers the snapshot this machine built for it
 * (spec §6.4, §7), else the member's own while it is indexed from it, else
 * nothing. Every other path is the member's own.
 */
@CompileStatic
class LocalSnapshotFiles implements MemberFiles {

    private final String alias
    private final MemberFiles own
    private final IndexJobs jobs

    LocalSnapshotFiles(String alias, MemberFiles own, IndexJobs jobs) {
        this.alias = alias
        this.own = own
        this.jobs = jobs
    }

    @Override
    MemberFiles.Opened open(String rel) {
        if( rel != IndexSnapshot.relativePath() )
            return own.open(rel)
        final Path local = jobs.localSnapshot(alias)
        if( local != null )
            // <localRoot>/<alias>/index/v7.sqlite: two levels up is the directory a member's root would be.
            return new LocalMemberFiles(local.parent.parent).open(rel)
        return jobs.servesOwn(alias) ? own.open(rel) : null
    }

    @Override
    List<String> list(String dirRel) { own.list(dirRel) }

    @Override
    String describe() { own.describe() }
}
