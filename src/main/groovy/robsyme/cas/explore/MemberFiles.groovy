package robsyme.cas.explore

/**
 * What the explorer's server needs of a store member (DESIGN.md §15): sizes,
 * ranged reads and one directory listing. Paths are relative to the member
 * root, `/`-separated; the server decides which ones may be asked for.
 */
interface MemberFiles {

    /** Size in bytes, or null when there is no such file. */
    Long size(String rel)

    /** The file's bytes from {@code start}; the caller reads at most {@code length}. */
    InputStream open(String rel, long start, long length)

    /** Names directly under {@code dirRel}, in no order; empty when there is no such directory. */
    List<String> list(String dirRel)

    /** For messages: where this member lives. */
    String describe()
}
