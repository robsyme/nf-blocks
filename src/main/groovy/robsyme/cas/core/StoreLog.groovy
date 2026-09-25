package robsyme.cas.core

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * The Store Log (DESIGN.md §5, block explorer spec section 3): one write-once
 * empty entry per block written outside a run's closure, under `log/`, named
 * `String.format('%013d', 9999999999999L - writtenAtMillis)`, then the kind,
 * then the address, so a listing sorts newest first. `writtenAtMillis` is when
 * the entry was written to this member, so a merged Bundle's runs sort as new.
 *
 * Derived, like the index: a failure here is logged and never aborts a run
 * (DESIGN.md §0 rule 3).
 */
@Slf4j
@CompileStatic
class StoreLog {

    /** One past the last millisecond this encoding can carry. */
    static final long HORIZON_MILLIS = 9999999999999L

    /**
     * How far before the watermark a catch-up re-reads, so an entry written
     * behind it by a host with a skewed clock is not skipped.
     */
    static final long OVERLAP_MILLIS = 600_000L

    private final StoreLogStorage storage

    StoreLog(StoreLogStorage storage) {
        this.storage = storage
    }

    /** The Store Log of a store's writable member. */
    static StoreLog of(BlockStore store) {
        return new StoreLog(storageOf(store))
    }

    private static StoreLogStorage storageOf(BlockStore store) {
        if( store instanceof LocalBlockStore )
            return new LocalStoreLogStorage(((LocalBlockStore) store).getRoot())
        if( store instanceof CompositeStore )
            return storageOf(((CompositeStore) store).getMembers()[0])
        throw new IllegalArgumentException("no store log storage for ${store?.getClass()?.name}")
    }

    static void append(BlockStore store, StoreLogKind kind, Cid cid, long writtenAtMillis) {
        of(store).append(kind, cid, writtenAtMillis)
    }

    static List<StoreLogEntry> read(BlockStore store) {
        return of(store).read()
    }

    /** The entry name, which is also its watermark form. */
    static String entryName(StoreLogKind kind, Cid cid, long writtenAtMillis) {
        return reverseTimestamp(writtenAtMillis) + '-' + kind.token + '-' + cid.toString()
    }

    static String reverseTimestamp(long writtenAtMillis) {
        final long reverse = HORIZON_MILLIS - writtenAtMillis
        if( reverse < 0 || writtenAtMillis < 0 )
            throw new IllegalArgumentException("a store log timestamp must fall between 0 and $HORIZON_MILLIS, got $writtenAtMillis")
        return String.format('%013d', reverse)
    }

    void append(StoreLogKind kind, Cid cid, long writtenAtMillis) {
        storage.putEntry(entryName(kind, cid, writtenAtMillis))
    }

    /** Every entry, newest first. Names that do not parse are ignored. */
    List<StoreLogEntry> read() {
        final List<StoreLogEntry> entries = new ArrayList<StoreLogEntry>()
        for( String name : storage.listEntries() ) {
            final StoreLogEntry entry = parse(name)
            if( entry != null )
                entries.add(entry)
        }
        Collections.sort(entries)
        return entries
    }

    /**
     * The entries a catch-up must consider, newest first: everything newer
     * than the watermark, the watermark itself, and everything written up to
     * {@link #OVERLAP_MILLIS} before it. A null or empty watermark means
     * everything. The caller skips what it has already ingested.
     */
    List<StoreLogEntry> entriesSince(String watermark) {
        return entriesSince(watermark, System.currentTimeMillis())
    }

    /**
     * As {@link #entriesSince(String)}, with the local clock given. The floor
     * is taken from the earlier of the watermark and now, so a watermark
     * written by a host whose clock runs ahead cannot hide entries that
     * correctly clocked hosts write behind it.
     */
    List<StoreLogEntry> entriesSince(String watermark, long nowMillis) {
        final List<StoreLogEntry> entries = read()
        if( !watermark )
            return entries
        final StoreLogEntry mark = parse(watermark)
        if( mark == null ) {
            log.warn("ignoring an unreadable store log watermark '$watermark'; reading the whole log")
            return entries
        }
        final long floor = Math.min(mark.writtenAtMillis, nowMillis) - OVERLAP_MILLIS
        final List<StoreLogEntry> since = new ArrayList<StoreLogEntry>()
        for( StoreLogEntry entry : entries )
            if( entry.writtenAtMillis >= floor )
                since.add(entry)
        return since
    }

    static StoreLogEntry parse(String name) {
        final String[] parts = name.split('-', 3)
        if( parts.length != 3 || !parts[0].matches(/\d{13}/) ) {
            log.warn("ignoring a store log entry that is not <13 digits>-<kind>-<cid>: '$name'")
            return null
        }
        final StoreLogKind kind = StoreLogKind.fromToken(parts[1])
        if( kind == null ) {
            log.warn("ignoring a store log entry of unknown kind '${parts[1]}': '$name'")
            return null
        }
        if( !Cid.isCid(parts[2]) ) {
            log.warn("ignoring a store log entry whose name does not end in a cid: '$name'")
            return null
        }
        final long written = HORIZON_MILLIS - Long.parseLong(parts[0])
        return new StoreLogEntry(name, parts[0], written, kind, Cid.parse(parts[2]))
    }
}
