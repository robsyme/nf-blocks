package robsyme.cas.core

import groovy.transform.CompileStatic

/**
 * Read-only view of a member's Nextflow lineage records, `nf/<key>/.data.json`
 * (DESIGN.md §5, DefaultLinStore layout). Keys may contain '/', and a key's
 * directory may also hold child keys. Nothing here writes.
 */
@CompileStatic
interface LineageRecords {

    /** Record keys below the directory prefix (e.g. `<hash>/`), in key order. A record whose key is the prefix itself is not below it. */
    List<String> keysUnder(String prefix)

    /**
     * The decoded `.data.json` of one record; null when there is none.
     * @throws IllegalStateException naming the key, when it is not a JSON object
     */
    Map read(String key)
}
