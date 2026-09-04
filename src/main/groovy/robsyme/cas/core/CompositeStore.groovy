package robsyme.cas.core

import java.util.stream.Stream

import groovy.transform.CompileStatic

/**
 * Several stores read as one (DESIGN.md §5). Reads resolve in member order
 * and the first hit wins. Writes go to member 0 only, and are never skipped
 * because a read-only member happens to hold the block: the writable member
 * must be able to serve everything this run produced on its own.
 */
@CompileStatic
class CompositeStore implements BlockStore {

    private final List<BlockStore> members

    CompositeStore(List<BlockStore> members) {
        if( !members )
            throw new IllegalArgumentException('a composite store needs at least one member')
        if( !members[0].isWritable() )
            throw new IllegalArgumentException("the first member of a composite store must be writable, but '${members[0].alias()}' is not")
        this.members = List.copyOf(members)
    }

    List<BlockStore> getMembers() { members }

    private BlockStore writer() { members[0] }

    private BlockStore holderOf(Cid cid) {
        for( BlockStore member : members )
            if( member.has(cid) )
                return member
        throw new NoSuchBlockException(cid, members*.alias().join(', '))
    }

    @Override
    String alias() { writer().alias() }

    @Override
    boolean isWritable() { true }

    @Override
    boolean has(Cid cid) {
        for( BlockStore member : members )
            if( member.has(cid) )
                return true
        return false
    }

    @Override
    long size(Cid cid) { holderOf(cid).size(cid) }

    @Override
    InputStream open(Cid cid) { holderOf(cid).open(cid) }

    @Override
    long lastModifiedMillis(Cid cid) { holderOf(cid).lastModifiedMillis(cid) }

    @Override
    void put(Cid cid, InputStream input, long expectedSize) { writer().put(cid, input, expectedSize) }

    @Override
    Cid putStreaming(InputStream input) { writer().putStreaming(input) }

    @Override
    Cid putDagCbor(Object value) { writer().putDagCbor(value) }

    @Override
    Stream<Cid> listBlocks() {
        Stream<Cid> all = Stream.<Cid> empty()
        for( BlockStore member : members )
            all = Stream.concat(all, member.listBlocks())
        return all.distinct()
    }

    @Override
    String toString() { "CompositeStore${members*.alias()}" }
}
