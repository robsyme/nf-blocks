package robsyme.cas.core

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * The Run Log (DESIGN.md §5 and §12): one write-once empty entry per
 * RunCompletion under `runs/`, named
 * `String.format('%013d', 9999999999999L - finishedAtMillis)` then the
 * RunCompletion address, so a listing sorts newest first and an incremental
 * pass reads only what lies past a watermark.
 *
 * Derived, like the index: it can be rebuilt by scanning `bafy…` blocks, so a
 * failure here is logged and never aborts a run (DESIGN.md §0 rule 3).
 */
@Slf4j
@CompileStatic
class RunLog {

    /** One past the last millisecond this encoding can carry. */
    static final long HORIZON_MILLIS = 9999999999999L

    private final RunLogStorage storage

    RunLog(RunLogStorage storage) {
        this.storage = storage
    }

    /** The Run Log of a store's writable member. */
    static RunLog of(BlockStore store) {
        return new RunLog(storageOf(store))
    }

    private static RunLogStorage storageOf(BlockStore store) {
        if( store instanceof LocalBlockStore )
            return new LocalRunLogStorage(((LocalBlockStore) store).getRoot())
        if( store instanceof CompositeStore )
            return storageOf(((CompositeStore) store).getMembers()[0])
        throw new IllegalArgumentException("no run log storage for ${store?.getClass()?.name}")
    }

    static void append(BlockStore store, Cid completion, long finishedAtMillis) {
        of(store).append(completion, finishedAtMillis)
    }

    static List<RunLogEntry> read(BlockStore store) {
        return of(store).read()
    }

    /** The entry name for a completion, which is also its watermark form. */
    static String entryName(Cid completion, long finishedAtMillis) {
        return reverseTimestamp(finishedAtMillis) + '-' + completion.toString()
    }

    static String reverseTimestamp(long finishedAtMillis) {
        final long reverse = HORIZON_MILLIS - finishedAtMillis
        if( reverse < 0 || finishedAtMillis < 0 )
            throw new IllegalArgumentException("a run log timestamp must fall between 0 and $HORIZON_MILLIS, got $finishedAtMillis")
        return String.format('%013d', reverse)
    }

    void append(Cid completion, long finishedAtMillis) {
        storage.putEntry(entryName(completion, finishedAtMillis))
    }

    /** Every entry, newest first. Names that do not parse are ignored. */
    List<RunLogEntry> read() {
        final List<RunLogEntry> entries = new ArrayList<RunLogEntry>()
        for( String name : storage.listEntries() ) {
            final RunLogEntry entry = parse(name)
            if( entry != null )
                entries.add(entry)
        }
        Collections.sort(entries)
        return entries
    }

    /**
     * The entries newer than the watermark, newest first. The watermark is an
     * entry name; a null or empty one means everything.
     */
    List<RunLogEntry> entriesAfter(String watermark) {
        final List<RunLogEntry> entries = read()
        if( !watermark )
            return entries
        // Newest first means a newer entry sorts before the watermark.
        final List<RunLogEntry> newer = new ArrayList<RunLogEntry>()
        for( RunLogEntry entry : entries )
            if( entry.name < watermark )
                newer.add(entry)
        return newer
    }

    static RunLogEntry parse(String name) {
        final int dash = name.indexOf((int) ('-' as char))
        if( dash != 13 ) {
            log.warn("ignoring a run log entry that is not <13 digits>-<cid>: '$name'")
            return null
        }
        final String reverseTs = name.substring(0, dash)
        if( !reverseTs.matches(/\d{13}/) ) {
            log.warn("ignoring a run log entry whose reverse timestamp is not 13 digits: '$name'")
            return null
        }
        final String cidText = name.substring(dash + 1)
        if( !Cid.isCid(cidText) ) {
            log.warn("ignoring a run log entry whose name is not a cid: '$name'")
            return null
        }
        return new RunLogEntry(name, reverseTs, HORIZON_MILLIS - Long.parseLong(reverseTs), Cid.parse(cidText))
    }
}
