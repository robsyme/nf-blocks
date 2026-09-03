package robsyme.cas.core

/**
 * The one thing the Run Log needs of a store: write-once empty entries under
 * a `runs/` prefix and list them (DESIGN.md §5). Kept separate from
 * {@link BlockStore} because a Run Log entry is not a block, and small enough
 * that an object-store member can implement it later without a block store.
 */
interface RunLogStorage {

    /** Creates the empty entry. Already present is success. */
    void putEntry(String name)

    /** Every entry name, in no particular order. */
    List<String> listEntries()
}
