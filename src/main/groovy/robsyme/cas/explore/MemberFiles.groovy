package robsyme.cas.explore

/**
 * What the explorer's server needs of a store member (DESIGN.md §15): files
 * opened once for ranged reads, and one directory listing. Paths are relative
 * to the member root, `/`-separated; the server decides which ones may be
 * asked for.
 */
interface MemberFiles {

    /**
     * The file, opened once, or null when there is no such file. Its size and
     * tag are the opened file's, never a second look at the path, so a file
     * replaced meanwhile cannot be answered with one file's size and another's
     * bytes.
     */
    Opened open(String rel)

    /** Names directly under {@code dirRel}, in no order; empty when there is no such directory. */
    List<String> list(String dirRel)

    /** For messages: where this member lives. */
    String describe()

    /** One opened version of a member file. */
    interface Opened extends Closeable {

        long getSize()

        /** A strong HTTP entity tag, quoted; a replaced file has a different one. */
        String getTag()

        /** The opened file's bytes from {@code start}; the caller reads at most {@code length}. */
        InputStream read(long start, long length)
    }
}
