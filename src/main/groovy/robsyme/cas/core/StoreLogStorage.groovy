package robsyme.cas.core

/**
 * The one thing the Store Log needs of a store: write-once empty entries under
 * a `log/` prefix and list them (DESIGN.md §5). Kept separate from
 * {@link BlockStore} because an entry is not a block.
 */
interface StoreLogStorage {

    /** Creates the empty entry. Already present is success. */
    void putEntry(String name)

    /** Every entry name, in no particular order. */
    List<String> listEntries()
}
