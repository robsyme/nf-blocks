package robsyme.cas.core

import java.nio.file.AccessDeniedException
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.regex.Pattern

import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * The one builder behind `nf-blocks:put` and `POST /api/put` (block explorer
 * spec section 9). A block is a pure function of the request and this
 * server's asserted_by, and its address is known before anything is checked
 * against the store. Order, which decision 9 of the milestone 2 plan fixes:
 * shape, address, dry run, "already written" (success, unchanged), then
 * membership, supersedes and clock, then write, Store Log entry, ingest.
 */
@Slf4j
@CompileStatic
class Put {

    static final long MAX_BLOCK_BYTES = 1024L * 1024
    static final long MAX_REQUEST_BYTES = 2L * 1024 * 1024
    static final long CLOCK_SKEW_MILLIS = 600_000L
    static final int MAX_NAME_CHARS = 256

    private static final Pattern TIMESTAMP = ~/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/
    private static final Set<String> SELECTION_KEYS = ['kind', 'members', 'derived_from'] as Set
    private static final Set<String> CLAIM_KEYS = ['kind', 'subject', 'verb', 'attribute', 'value', 'supersedes', 'timestamp'] as Set
    private static final Set<String> ITEM_KEYS = ['address', 'via'] as Set
    private static final Set<String> CLAIM_VERBS = [Claim.SET, Claim.DELETE, Claim.DEL] as Set

    /** A request turned into a block, and what to check before writing it. */
    @Canonical
    @CompileStatic
    private static final class Draft {
        Map<String, Object> block
        StoreLogKind logKind
        /** Where a too_large refusal points, and the backstop encode refusal below (final review finding 1). */
        String sizeAt
        Closure validate
    }

    /** One member entry as the request wrote it, with the pointers its errors name. */
    @Canonical
    @CompileStatic
    private static final class Entry {
        Selection.Member member
        String addressAt
        List<Cid> requestVia
        List<String> viaAt
    }

    private final BlockStore store
    private final BlockStore writable
    private final Index index
    private final String assertedBy
    private final Closure<Long> clock
    private final Closure catchUp

    Put(BlockStore store, BlockStore writable, Index index, String assertedBy, Closure<Long> clock, Closure catchUp) {
        this.store = store
        this.writable = writable
        this.index = index
        this.assertedBy = assertedBy
        this.clock = clock
        this.catchUp = catchUp
    }

    synchronized PutResult put(byte[] body, boolean dryRun) {
        if( body.length > MAX_REQUEST_BYTES )
            throw new PutError(PutError.TOO_LARGE, "the request is ${body.length} bytes, over ${MAX_REQUEST_BYTES}", '')
        final Object request
        try {
            request = DagJson.decode(body)
        }
        catch( DagJson.DagJsonException e ) {
            throw new PutError(PutError.INVALID, "the request is not DAG-JSON: ${e.message}", e.at)
        }
        return put(request, dryRun)
    }

    synchronized PutResult put(Object request, boolean dryRun) {
        if( !(request instanceof Map) )
            throw invalid("the request is a DAG-JSON map with a kind, 'Selection' or 'Claim'", '')
        final Map map = (Map) request
        final Object kind = map.get('kind')
        if( kind == null )
            throw invalid("the request needs a kind, 'Selection' or 'Claim'", '/kind')
        final Draft draft
        if( kind == Records.SELECTION )
            draft = selectionDraft(map)
        else if( kind == Records.CLAIM )
            draft = claimDraft(map)
        else
            throw new PutError(PutError.WRONG_KIND, "a client may build a Selection or a Claim, not ${kind}", '/kind')

        final byte[] bytes
        try {
            bytes = DagCbor.encode(draft.block)
        }
        catch( IllegalArgumentException e ) {
            // Backstop (final review finding 1): DagJson already refuses a lone
            // surrogate or an out-of-range integer while decoding a request, but
            // a caller that builds the request as a Map directly, bypassing
            // DagJson, can still reach content dag-cbor cannot encode.
            throw invalid("the request cannot be encoded: ${e.message}", draft.sizeAt)
        }
        if( bytes.length > MAX_BLOCK_BYTES )
            throw new PutError(PutError.TOO_LARGE, "the encoded ${kind} is ${bytes.length} bytes, over ${MAX_BLOCK_BYTES}; " +
                'split it into nested Selections', draft.sizeAt)
        final Cid address = DagCbor.cidOf(bytes)
        catchUp.call()
        if( dryRun ) {
            final ClaimState state = index.claimState(address)
            return PutResult.dryRun(address, store.has(address), writable.has(address), state.names,
                state.nameClaims.collect { String c -> Cid.parse(c) })
        }
        if( !writable.isWritable() )
            throw new PutError(PutError.NOT_WRITABLE, "store member '${writable.alias()}' is not writable", '')
        if( writable.has(address) )
            return PutResult.written(address, draft.block, entryFor(address, draft.logKind), false)
        draft.validate.call()
        try {
            writable.put(address, new ByteArrayInputStream(bytes), (long) bytes.length)
        }
        catch( AccessDeniedException e ) {
            throw new PutError(PutError.NOT_WRITABLE, "store member '${writable.alias()}' refused the write: ${e.message}", '')
        }
        final String entry = append(address, draft.logKind)
        ingest()
        return PutResult.written(address, draft.block, entry, true)
    }

    // ------------------------------------------------------------------ Selection

    private Draft selectionDraft(Map map) {
        unknownKeys(map, SELECTION_KEYS, '')
        final Object members = map.get('members')
        if( !(members instanceof List) )
            throw invalid('members is a list', '/members')
        if( ((List) members).isEmpty() )
            throw new PutError(PutError.EMPTY, 'an empty Selection is refused (block explorer spec section 7.2)', '/members')
        final List<Entry> entries = new ArrayList<Entry>()
        final List list = (List) members
        for( int i = 0; i < list.size(); i++ )
            entries.add(entryOf(list[i], "/members/${i}".toString()))
        final List<Cid> derived = derivedFrom(map.get('derived_from'))
        final Selection selection
        try {
            selection = new Selection(assertedBy, entries.collect { Entry e -> e.member }, derived)
        }
        catch( IllegalArgumentException e ) {
            throw invalid(e.message, '/members')
        }
        return new Draft(selection.toCbor(), StoreLogKind.SELECTION, '/members', { -> validateSelection(entries) })
    }

    private static Entry entryOf(Object m, String at) {
        if( m instanceof String ) {
            final ItemOccurrence o = ItemOccurrence.parse((String) m)
            if( o == null || o.leaf != null )
                throw invalid("a member written as text is an Item Occurrence, cas://<collection>/<item>, got '${m}'", at)
            return new Entry(Selection.item(o.item, [o.collection]), at, [o.collection], [at])
        }
        if( !(m instanceof Map) || ((Map) m).size() != 1 )
            throw invalid('a member is {"item": {"address", "via"}}, {"selection": <link>} or an Item Occurrence URI', at)
        final Map entry = (Map) m
        if( entry.containsKey('selection') ) {
            if( !(entry.get('selection') instanceof Cid) )
                throw invalid('selection is a link', "${at}/selection".toString())
            return new Entry(Selection.selection((Cid) entry.get('selection')), "${at}/selection".toString(), [], [])
        }
        if( entry.get('item') instanceof Map ) {
            final Map item = (Map) entry.get('item')
            final String here = "${at}/item".toString()
            unknownKeys(item, ITEM_KEYS, here)
            if( !(item.get('address') instanceof Cid) )
                throw invalid('item.address is a link', "${here}/address".toString())
            final Object via = item.containsKey('via') ? item.get('via') : []
            if( !(via instanceof List) || !((List) via).every { Object v -> v instanceof Cid } )
                throw invalid('item.via is a list of links', "${here}/via".toString())
            final List<Cid> requestVia = (List<Cid>) via
            final List<String> viaAt = new ArrayList<String>()
            for( int j = 0; j < requestVia.size(); j++ )
                viaAt.add("${here}/via/${j}".toString())
            return new Entry(Selection.item((Cid) item.get('address'), requestVia), "${here}/address".toString(), requestVia, viaAt)
        }
        throw invalid('a member is {"item": {"address", "via"}}, {"selection": <link>} or an Item Occurrence URI', at)
    }

    /** decision 8: links or bytes in, binary CIDs out. */
    private static List<Cid> derivedFrom(Object value) {
        if( value == null )
            return []
        if( !(value instanceof List) )
            throw invalid('derived_from is a list of links', '/derived_from')
        final List<Cid> out = new ArrayList<Cid>()
        final List list = (List) value
        for( int i = 0; i < list.size(); i++ ) {
            final Object d = list[i]
            if( d instanceof Cid ) {
                out.add((Cid) d)
                continue
            }
            if( d instanceof byte[] ) {
                try {
                    out.add(Cid.fromBytes((byte[]) d))
                    continue
                }
                catch( IllegalArgumentException e ) {
                    throw invalid("derived_from/${i} is not a binary CID: ${e.message}", "/derived_from/${i}".toString())
                }
            }
            throw invalid('derived_from holds links or binary CIDs', "/derived_from/${i}".toString())
        }
        return out
    }

    private void validateSelection(List<Entry> entries) {
        for( Entry e : entries ) {
            final Selection.Member m = e.member
            final String kind = kindAt(m.address, e.addressAt)
            final String wanted = m.nested ? Records.SELECTION : Records.OUTPUT_ITEM
            if( kind != wanted )
                throw new PutError(PutError.WRONG_KIND, "${m.address} is ${describe(kind)}, not ${m.nested ? 'a Selection' : 'an Output Item'}", e.addressAt)
            for( int j = 0; j < e.requestVia.size(); j++ ) {
                final Cid via = e.requestVia[j]
                final Map collection = blockAt(via, e.viaAt[j])
                if( Records.kindOf(collection) != Records.OUTPUT_COLLECTION )
                    throw new PutError(PutError.WRONG_KIND, "${via} is ${describe(Records.kindOf(collection))}, not an Output Collection", e.viaAt[j])
                if( !((List) collection.get('items') ?: []).contains(m.address) )
                    throw new PutError(PutError.NOT_IN_VIA, "collection ${via} does not list item ${m.address}", e.viaAt[j])
            }
        }
    }

    // ---------------------------------------------------------------------- Claim

    private Draft claimDraft(Map map) {
        unknownKeys(map, CLAIM_KEYS, '')
        final Object subject = map.get('subject')
        if( !(subject instanceof Cid) )
            throw invalid('subject is a link', '/subject')
        final Object verb = map.get('verb')
        if( verb == Claim.ADD )
            throw invalid("verb 'add' is not built yet (block explorer spec section 8: out of the Claims slice)", '/verb')
        if( !(verb instanceof String) || !CLAIM_VERBS.contains((String) verb) )
            throw invalid('verb is one of set, delete, del', '/verb')
        final Object attribute = map.get('attribute')
        if( attribute != null && !(attribute instanceof String && ((String) attribute)) )
            throw invalid('attribute is a non-empty string or null', '/attribute')
        final Object value = map.get('value')
        final Object supersedesValue = map.containsKey('supersedes') ? map.get('supersedes') : []
        if( !(supersedesValue instanceof List) || !((List) supersedesValue).every { Object s -> s instanceof Cid } )
            throw invalid('supersedes is a list of links', '/supersedes')
        final List<Cid> supersedes = (List<Cid>) supersedesValue
        final Object timestamp = map.get('timestamp')
        if( !(timestamp instanceof String) || !TIMESTAMP.matcher((String) timestamp).matches() )
            throw invalid('timestamp is ISO-8601 UTC with milliseconds, e.g. 2026-09-25T10:00:00.000Z', '/timestamp')
        long timestampMillis
        try {
            timestampMillis = Instant.parse((String) timestamp).toEpochMilli()
        }
        catch( DateTimeParseException e ) {
            throw invalid("timestamp '${timestamp}' is not a real instant", '/timestamp')
        }
        switch( (String) verb ) {
            case Claim.SET:
                if( attribute == null ) throw invalid('set needs an attribute', '/attribute')
                if( value == null ) throw invalid('set needs a value', '/value')
                if( attribute == Claim.NAME && !(value instanceof String && ((String) value).trim() && ((String) value).length() <= MAX_NAME_CHARS) )
                    throw invalid("a name is a non-blank string of at most ${MAX_NAME_CHARS} characters", '/value')
                break
            case Claim.DELETE:
                if( attribute != null ) throw invalid('delete names the subject itself, with no attribute', '/attribute')
                if( value != null ) throw invalid('delete has no value', '/value')
                break
            case Claim.DEL:
                if( value != null ) throw invalid('del has no value', '/value')
                if( supersedes.isEmpty() ) throw invalid('del supersedes the Claim it undoes', '/supersedes')
                break
        }
        final Claim claim = new Claim(assertedBy, (Cid) subject, (String) verb, (String) attribute, value, supersedes, (String) timestamp)
        // '/value' is the most likely place for content dag-cbor cannot encode
        // (an arbitrary Claim value) and, for too_large, as good a guess as any.
        return new Draft(claim.toCbor(), StoreLogKind.CLAIM, '/value', { -> validateClaim(claim, supersedes, timestampMillis) })
    }

    private void validateClaim(Claim claim, List<Cid> requested, long timestampMillis) {
        final long now = clock.call()
        if( Math.abs(now - timestampMillis) > CLOCK_SKEW_MILLIS )
            throw new PutError(PutError.CLOCK_SKEW, "timestamp ${claim.timestamp} is more than 10 minutes from this server's clock (${Index.isoMillis(now)})", '/timestamp')
        for( int j = 0; j < requested.size(); j++ ) {
            final Cid s = requested[j]
            final String at = "/supersedes/${j}".toString()
            if( !store.has(s) )
                throw new PutError(PutError.STALE_SUPERSEDES, "claim ${s} is not in this composition; reload and try again", at)
            final Map block = blockAt(s, at)
            if( Records.kindOf(block) != Records.CLAIM )
                throw new PutError(PutError.WRONG_KIND, "${s} is ${describe(Records.kindOf(block))}, not a Claim", at)
            if( block.get('subject') != claim.subject )
                throw new PutError(PutError.WRONG_KIND, "claim ${s} is about ${block.get('subject')}, not ${claim.subject}", at)
            final List<Cid> by = index.supersedersOf(s, writable.alias())
            if( by )
                throw new PutError(PutError.STALE_SUPERSEDES, "claim ${s} is already superseded by ${by.join(', ')}; reload and try again", at)
        }
    }

    // ------------------------------------------------------------------ plumbing

    private Map blockAt(Cid cid, String at) {
        if( !store.has(cid) )
            throw new PutError(PutError.NOT_FOUND, "${cid} is not in any member of this composition", at)
        if( cid.isRaw() )
            return [:]
        final InputStream input = store.open(cid)
        try {
            final Object value = DagCbor.decode(input.readAllBytes())
            return value instanceof Map ? (Map) value : [:]
        }
        finally {
            input.close()
        }
    }

    private String kindAt(Cid cid, String at) {
        return Records.kindOf(blockAt(cid, at))
    }

    private static String describe(String kind) {
        return kind == null ? 'not a metadata block' : "a ${kind}"
    }

    private String entryFor(Cid address, StoreLogKind kind) {
        final StoreLogEntry existing = index.firstLogEntry(address, writable.alias())
        return existing != null ? existing.name : append(address, kind)
    }

    private String append(Cid address, StoreLogKind kind) {
        final long now = clock.call()
        StoreLog.append(writable, kind, address, now)
        return StoreLog.entryName(kind, address, now)
    }

    /** The write is done; the index is derived, so a failure here only warns (DESIGN.md §0 rule 3). */
    private void ingest() {
        try {
            index.catchUp(writable, StoreLog.of(writable), writable.alias())
        }
        catch( Exception e ) {
            log.warn("the index could not ingest a block just written; it is derived and catches up later: ${e.message}")
        }
    }

    private static void unknownKeys(Map map, Set<String> known, String at) {
        for( Object key : map.keySet() )
            if( !known.contains(key) )
                throw invalid("unknown field '${key}'; this takes ${known.join(', ')}", "${at}/${DagJson.pointer(String.valueOf(key))}".toString())
    }

    private static PutError invalid(String message, String at) {
        return new PutError(PutError.INVALID, message, at)
    }
}
