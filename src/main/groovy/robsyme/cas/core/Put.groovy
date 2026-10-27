package robsyme.cas.core

import java.nio.file.AccessDeniedException
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.function.Supplier
import java.util.regex.Pattern

import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * The one builder behind `nf-blocks:put` and `POST /api/put` (block explorer
 * spec section 9). A block is a pure function of the request and this
 * server's asserted_by. Order (decision 9 of the milestone 2 plan, with the
 * store-level checks moved ahead of signing): shape and size; for a write,
 * the checks that need no address (kinds, presence, membership); then sign
 * and address, dry run, "already written" (success, unchanged), clock and
 * the index's supersedes check, then write, Store Log entry, ingest. A
 * request refused before signing resolves no identity. The clock and the
 * supersedes check stay after "already written": a retry must dedupe first,
 * and the address it dedupes by holds the name.
 */
@Slf4j
@CompileStatic
class Put {

    static final long MAX_BLOCK_BYTES = 1024L * 1024
    static final long MAX_REQUEST_BYTES = 2L * 1024 * 1024
    static final long CLOCK_SKEW_MILLIS = 600_000L
    static final int MAX_NAME_CHARS = 256
    static final int MAX_NOTE_CHARS = 256
    /** A SampleRef's strings, an annotation's value, a policy's string (P4 plan 1.2). */
    static final int MAX_SAMPLE_CHARS = 256

    private static final Pattern TIMESTAMP = ~/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/
    private static final Set<String> SELECTION_KEYS = ['kind', 'members', 'derived_from'] as Set
    private static final Set<String> CLAIM_KEYS = ['kind', 'subject', 'verb', 'attribute', 'value', 'supersedes', 'timestamp'] as Set
    private static final Set<String> PROJECT_KEYS = ['kind', 'name', 'description', 'created_at'] as Set
    private static final Set<String> ITEM_KEYS = ['address', 'via'] as Set
    private static final Set<String> CLAIM_VERBS = [Claim.SET, Claim.ADD, Claim.DELETE, Claim.DEL] as Set
    /**
     * What a pin may name (ticket 21 answer 6: pins on any subject whose
     * content a sweep could take); a raw block is any held raw address. A
     * pinned Selection keeps its closure, even when deleted, as a pinned run does.
     */
    private static final Set<String> PINNABLE = [Records.RUN_COMPLETION, Records.OUTPUT_COLLECTION,
        Records.OUTPUT_ITEM, Records.DIRECTORY_MANIFEST, Records.SELECTION] as Set
    /** The attributes whose values are sets, so `add` (ticket 21 answer 2, P4 plan 1.2). */
    private static final Set<String> ADD_ATTRIBUTES = [Claim.PIN, Claim.PROJECT, Claim.SAMPLE, Claim.ANNOTATION,
        Claim.SAME_SAMPLE, Claim.POLICY_PEOPLE] as Set
    /** The only attributes a Claim about a Project carries (P4 plan 1.2); null is delete and its undo. */
    private static final Set<String> ON_A_PROJECT = [Claim.SAMPLE, Claim.ANNOTATION, Claim.SAME_SAMPLE, Claim.POLICY_DEFAULT_STORE,
        Claim.POLICY_RETENTION, Claim.POLICY_PEOPLE, Claim.NAME, null] as Set
    /** What `add project` makes a member of a project. */
    private static final Set<String> PROJECT_MEMBERS = [Records.RUN_COMPLETION, Records.SELECTION, Records.OUTPUT_ITEM] as Set
    private static final Set<String> RETENTION_VALUES = ['pin_selections', 'none'] as Set
    private static final Set<String> SAMPLE_REF_KEYS = ['pipeline', 'id'] as Set
    private static final Set<String> OCCURRENCE_KEYS = ['pipeline', 'id', 'run'] as Set
    private static final Set<String> ANNOTATION_KEYS = ['sample', 'name', 'value'] as Set
    private static final Set<String> SAME_SAMPLE_KEYS = ['a', 'b', 'same'] as Set
    private static final Pattern ANNOTATION_NAME = ~/^[A-Za-z_][A-Za-z0-9_.]{0,63}$/

    /** A request turned into a block once someone signs it, and what to check before writing it. */
    @Canonical
    @CompileStatic
    private static final class Draft {
        /** The block, signed by the name it is given. */
        Closure<Map<String, Object>> sign
        StoreLogKind logKind
        /** Where a too_large refusal points, and the backstop encode refusal below (final review finding 1). */
        String sizeAt
        /** Kinds, presence and membership: they need no address, so they run before anyone signs. */
        Closure check
        /** The clock and the index's supersedes check: after "already written", so a retry dedupes first. */
        Closure validate

        Map<String, Object> signedBy(String who) {
            return (Map<String, Object>) sign.call(who)
        }
    }

    /**
     * Signs the probe that checks shape and size before the real name is
     * asked for. One character, the shortest name a block takes, so the probe
     * never encodes larger than the signed block: too_large for the probe is
     * too_large for any name.
     */
    private static final String PROBE_SIGNER = 'x'

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
    /** Who signs, asked once the request passed its shape, size and store-level checks (see the class comment). */
    private final Supplier<String> assertedBy
    private final Closure<Long> clock
    private final Closure catchUp

    Put(BlockStore store, BlockStore writable, Index index, Supplier<String> assertedBy, Closure<Long> clock, Closure catchUp) {
        this.store = store
        this.writable = writable
        this.index = index
        this.assertedBy = assertedBy
        this.clock = clock
        this.catchUp = catchUp
    }

    /** Decodes outside the lock; see {@link #put(Object, boolean)}. */
    PutResult put(byte[] body, boolean dryRun) {
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

    /**
     * The block is signed outside the lock: signing asks {@code assertedBy},
     * which can wait on the first identity resolution, and a samplesheet
     * export (ExploreCommand) waits on this same lock. Before that, the shape
     * and size are checked on a probe signed {@link #PROBE_SIGNER}, and a
     * write's store-level checks run, so a request they refuse never asks who
     * signs. Everything that reads the store or the index runs under the lock.
     */
    PutResult put(Object request, boolean dryRun) {
        final Draft draft = draftOf(request)
        encode(draft, draft.signedBy(PROBE_SIGNER))
        if( !dryRun ) {
            requireWritable()
            synchronized( this ) {
                draft.check.call()
            }
        }
        final Map<String, Object> block = draft.signedBy(assertedBy.get())
        synchronized( this ) {
            return write(draft, block, dryRun)
        }
    }

    private Draft draftOf(Object request) {
        if( !(request instanceof Map) )
            throw invalid("the request is a DAG-JSON map with a kind, 'Selection', 'Claim' or 'Project'", '')
        final Map map = (Map) request
        final Object kind = map.get('kind')
        if( kind == null )
            throw invalid("the request needs a kind, 'Selection', 'Claim' or 'Project'", '/kind')
        final Draft draft
        if( kind == Records.SELECTION )
            draft = selectionDraft(map)
        else if( kind == Records.CLAIM )
            draft = claimDraft(map)
        else if( kind == Records.PROJECT )
            draft = projectDraft(map)
        else
            throw new PutError(PutError.WRONG_KIND, "a client may build a Selection, a Claim or a Project, not ${kind}", '/kind')
        return draft
    }

    /** The block's dag-cbor bytes, or invalid or too_large. */
    private static byte[] encode(Draft draft, Map<String, Object> block) {
        final byte[] bytes
        try {
            bytes = DagCbor.encode(block)
        }
        catch( IllegalArgumentException e ) {
            // Backstop (final review finding 1): DagJson already refuses a lone
            // surrogate or an out-of-range integer while decoding a request, but
            // a caller that builds the request as a Map directly, bypassing
            // DagJson, can still reach content dag-cbor cannot encode.
            throw invalid("the request cannot be encoded: ${e.message}", draft.sizeAt)
        }
        if( bytes.length > MAX_BLOCK_BYTES )
            throw new PutError(PutError.TOO_LARGE, "the encoded ${block.get('kind')} is ${bytes.length} bytes, over ${MAX_BLOCK_BYTES}; " +
                'split it into nested Selections', draft.sizeAt)
        return bytes
    }

    private void requireWritable() {
        if( !writable.isWritable() )
            throw new PutError(PutError.NOT_WRITABLE, "store member '${writable.alias()}' is not writable", '')
    }

    private PutResult write(Draft draft, Map<String, Object> block, boolean dryRun) {
        final byte[] bytes = encode(draft, block)
        final Cid address = DagCbor.cidOf(bytes)
        catchUp.call()
        if( dryRun )
            return PutResult.dryRun(address, heldAnywhere(address), writable.has(address), index.claimState(address))
        if( writable.has(address) )
            return PutResult.written(address, block, entryFor(address, draft.logKind), false)
        draft.validate.call()
        try {
            writable.put(address, new ByteArrayInputStream(bytes), (long) bytes.length)
        }
        catch( AccessDeniedException e ) {
            throw new PutError(PutError.NOT_WRITABLE, "store member '${writable.alias()}' refused the write: ${e.message}", '')
        }
        final String entry = append(address, draft.logKind)
        ingest()
        return PutResult.written(address, block, entry, true)
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
        final List<Selection.Member> chosen = entries.collect { Entry e -> e.member }
        final Closure<Map<String, Object>> sign = { String who ->
            try {
                return new Selection(who, chosen, derived).toCbor()
            }
            catch( IllegalArgumentException e ) {
                throw invalid(e.message, '/members')
            }
        } as Closure<Map<String, Object>>
        return new Draft(sign, StoreLogKind.SELECTION, '/members', { -> validateSelection(entries) }, { -> })
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
        if( !(verb instanceof String) || !CLAIM_VERBS.contains((String) verb) )
            throw invalid('verb is one of set, add, delete, del', '/verb')
        final Object attribute = map.get('attribute')
        if( attribute != null && !(attribute instanceof String && ((String) attribute)) )
            throw invalid('attribute is a non-empty string or null', '/attribute')
        final Object value = map.get('value')
        final Object supersedesValue = map.containsKey('supersedes') ? map.get('supersedes') : []
        if( !(supersedesValue instanceof List) || !((List) supersedesValue).every { Object s -> s instanceof Cid } )
            throw invalid('supersedes is a list of links', '/supersedes')
        final List<Cid> supersedes = (List<Cid>) supersedesValue
        final Object timestamp = map.get('timestamp')
        final long timestampMillis = instantOf(timestamp, 'timestamp')
        switch( (String) verb ) {
            case Claim.SET:
                if( attribute == null ) throw invalid('set needs an attribute', '/attribute')
                if( value == null ) throw invalid('set needs a value', '/value')
                if( attribute == Claim.NAME && !(value instanceof String && ((String) value).trim() && ((String) value).length() <= MAX_NAME_CHARS) )
                    throw invalid("a name is a non-blank string of at most ${MAX_NAME_CHARS} characters", '/value')
                if( attribute == Claim.PIN )
                    throw invalid('a pin is add pin "<note>", never set', '/attribute')
                if( attribute in ADD_ATTRIBUTES )
                    throw invalid("${attribute} holds a set of values: add ${attribute}, never set", '/attribute')
                if( attribute == Claim.RETAIN && value != Claim.LINEAGE )
                    throw invalid('set retain takes the value "lineage" (keep the lineage, release the content)', '/value')
                if( attribute == Claim.POLICY_DEFAULT_STORE )
                    shortText(value, 'policy.default_store, a member alias,')
                if( attribute == Claim.POLICY_RETENTION && !(value in RETENTION_VALUES) )
                    throw invalid('policy.retention is "pin_selections" or "none"', '/value')
                break
            case Claim.ADD:
                if( !(attribute in ADD_ATTRIBUTES) )
                    throw invalid('add is for pin, project, sample, annotation, same_sample and policy.people', '/attribute')
                if( attribute == Claim.PIN ) {
                    if( !(value instanceof String && ((String) value).trim() && ((String) value).length() <= MAX_NOTE_CHARS) )
                        throw invalid("a pin's note is a non-blank string of at most ${MAX_NOTE_CHARS} characters", '/value')
                    if( !supersedes.isEmpty() ) throw invalid('add pin supersedes nothing; pins on one subject are a set', '/supersedes')
                }
                else
                    addValue((String) attribute, value)
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
        final Closure<Map<String, Object>> sign = { String who ->
            new Claim(who, (Cid) subject, (String) verb, (String) attribute, value, supersedes, (String) timestamp).toCbor()
        } as Closure<Map<String, Object>>
        // The checks read only the request's fields, never the signer, so they can run before anyone signs.
        final Claim claim = new Claim(PROBE_SIGNER, (Cid) subject, (String) verb, (String) attribute, value, supersedes, (String) timestamp)
        // '/value' is the most likely place for content dag-cbor cannot encode
        // (an arbitrary Claim value) and, for too_large, as good a guess as any.
        return new Draft(sign, StoreLogKind.CLAIM, '/value', { -> checkClaim(claim, supersedes) },
            { -> validateClaim(claim, supersedes, timestampMillis) })
    }

    /** Kinds and presence: what the store says of the subject and of each Claim superseded. */
    private void checkClaim(Claim claim, List<Cid> requested) {
        validateProjectClaim(claim)
        if( claim.attribute == Claim.RETAIN && kindAt(claim.subject, '/subject') != Records.RUN_COMPLETION )
            throw new PutError(PutError.WRONG_KIND, "retain names a run's RunCompletion; ${claim.subject} is ${describe(kindAt(claim.subject, '/subject'))}", '/subject')
        if( claim.attribute == Claim.PIN && !claim.subject.isRaw() && !(kindAt(claim.subject, '/subject') in PINNABLE) )
            throw new PutError(PutError.WRONG_KIND, "a pin names a run, collection, item, directory, file or Selection; ${claim.subject} is ${describe(kindAt(claim.subject, '/subject'))}", '/subject')
        if( claim.attribute == Claim.PIN && claim.subject.isRaw() && !store.has(claim.subject) )
            throw new PutError(PutError.NOT_FOUND, "${claim.subject} is not in any member of this composition", '/subject')
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
            // Both ways: a P4 Claim supersedes only its own attribute, and only its own attribute supersedes it.
            final Object supersededAttribute = block.get('attribute')
            if( isProjectAttribute(claim.attribute) && supersededAttribute != claim.attribute )
                throw new PutError(PutError.WRONG_KIND, "claim ${s} is ${describeClaim(block)}, not a ${claim.attribute} Claim", at)
            if( supersededAttribute instanceof String && isProjectAttribute((String) supersededAttribute) && supersededAttribute != claim.attribute )
                throw new PutError(PutError.WRONG_KIND, "claim ${s} is ${describeClaim(block)}, which only a ${supersededAttribute} Claim supersedes", at)
            if( claim.verb == Claim.DEL && claim.attribute in [Claim.RETAIN, Claim.PIN] ) {
                final boolean fits = block.get('attribute') == claim.attribute &&
                    (claim.attribute == Claim.RETAIN || block.get('verb') == Claim.ADD)
                if( !fits )
                    throw new PutError(PutError.WRONG_KIND, "claim ${s} is not ${claim.attribute == Claim.PIN ? 'an add pin' : 'a retain'} Claim", at)
            }
        }
    }

    /** The clock, then whether a Claim superseded is superseded already. */
    private void validateClaim(Claim claim, List<Cid> requested, long timestampMillis) {
        checkClock(claim.timestamp, timestampMillis, 'timestamp')
        for( int j = 0; j < requested.size(); j++ ) {
            final Cid s = requested[j]
            final List<Cid> by = index.supersedersOf(s, writable.alias())
            if( by )
                throw new PutError(PutError.STALE_SUPERSEDES, "claim ${s} is already superseded by ${by.join(', ')}; reload and try again", "/supersedes/${j}".toString())
        }
    }

    /**
     * The Claims on and about projects (P4 plan 1.2), against the store:
     * a Project takes only its own attributes, those attributes need a
     * Project, and the links in `add project` and `add same_sample` name
     * blocks of the right kind that the composition holds.
     */
    private void validateProjectClaim(Claim claim) {
        final String attribute = claim.attribute
        final boolean held = store.has(claim.subject)
        final String subjectKind = held ? kindAt(claim.subject, '/subject') : null
        if( subjectKind == Records.PROJECT && !(attribute in ON_A_PROJECT) )
            throw invalid("a Project takes sample, annotation, same_sample, policy.default_store, policy.retention, " +
                "policy.people, name and delete; not ${attribute}", '/attribute')
        if( isProjectScoped(attribute) ) {
            if( !held )
                throw new PutError(PutError.NOT_FOUND, "${claim.subject} is not in any member of this composition", '/subject')
            if( subjectKind != Records.PROJECT )
                throw new PutError(PutError.WRONG_KIND, "${attribute} is a Claim on a Project; ${claim.subject} is ${describe(subjectKind)}", '/subject')
        }
        if( claim.verb == Claim.ADD && attribute == Claim.PROJECT ) {
            if( !held )
                throw new PutError(PutError.NOT_FOUND, "${claim.subject} is not in any member of this composition", '/subject')
            if( !(subjectKind in PROJECT_MEMBERS) )
                throw new PutError(PutError.WRONG_KIND, "a project's member is a run, a Selection or an item; ${claim.subject} is ${describe(subjectKind)}", '/subject')
            final Cid project = (Cid) claim.value
            final String kind = kindAt(project, '/value')
            if( kind != Records.PROJECT )
                throw new PutError(PutError.WRONG_KIND, "${project} is ${describe(kind)}, not a Project", '/value')
        }
        if( claim.verb == Claim.ADD && attribute == Claim.SAME_SAMPLE ) {
            for( String side : ['a', 'b'] ) {
                final Cid run = (Cid) ((Map) ((Map) claim.value).get(side)).get('run')
                final String at = "/value/${side}/run".toString()
                final String kind = kindAt(run, at)
                if( kind != Records.RUN_COMPLETION )
                    throw new PutError(PutError.WRONG_KIND, "an Occurrence's run is a RunCompletion; ${run} is ${describe(kind)}", at)
            }
        }
    }

    /** sample, annotation, same_sample and policy.*: Claims that hang off a Project (P4 plan decision D1). */
    private static boolean isProjectScoped(String attribute) {
        return attribute in [Claim.SAMPLE, Claim.ANNOTATION, Claim.SAME_SAMPLE] || attribute?.startsWith('policy.')
    }

    /** The P4 attributes, whose Claims supersede, and are superseded by, only Claims of the same attribute. */
    private static boolean isProjectAttribute(String attribute) {
        return attribute == Claim.PROJECT || isProjectScoped(attribute)
    }

    private static String describeClaim(Map block) {
        final Object attribute = block.get('attribute')
        return attribute == null ? "a ${block.get('verb')} Claim" : "a ${attribute} Claim"
    }

    // ---------------------------------------------------------------- Claim values

    /** The value of an add other than pin (P4 plan 1.2); a refusal points at /value and names the field. */
    private static void addValue(String attribute, Object value) {
        switch( attribute ) {
            case Claim.PROJECT:
                if( !(value instanceof Cid) )
                    throw invalid('add project takes a link to the Project', '/value')
                break
            case Claim.SAMPLE:
                sampleRef(value, 'a sample')
                break
            case Claim.ANNOTATION:
                final Map annotation = fields(value, ANNOTATION_KEYS, 'an annotation')
                sampleRef(annotation.get('sample'), 'annotation.sample')
                final Object name = annotation.get('name')
                if( !(name instanceof String && ANNOTATION_NAME.matcher((String) name).matches()) )
                    throw invalid("annotation.name is a letter or _, then at most 63 letters, digits, _ or .; got ${render(name)}", '/value')
                final Object text = annotation.get('value')
                if( !(text instanceof String) || ((String) text).length() > MAX_SAMPLE_CHARS )
                    throw invalid("annotation.value is a string of at most ${MAX_SAMPLE_CHARS} characters", '/value')
                break
            case Claim.SAME_SAMPLE:
                final Map same = fields(value, SAME_SAMPLE_KEYS, 'a same_sample value')
                occurrence(same.get('a'), 'same_sample.a')
                occurrence(same.get('b'), 'same_sample.b')
                if( !(same.get('same') instanceof Boolean) )
                    throw invalid('same_sample.same is true (merge) or false (split)', '/value')
                break
            case Claim.POLICY_PEOPLE:
                shortText(value, 'policy.people, a principal,')
                break
        }
    }

    /** SampleRef = {pipeline, id}, both non-empty strings of at most 256 characters. */
    private static void sampleRef(Object value, String what) {
        final Map ref = fields(value, SAMPLE_REF_KEYS, what)
        shortText(ref.get('pipeline'), "${what}.pipeline")
        shortText(ref.get('id'), "${what}.id")
    }

    /** Occurrence = {pipeline, id, run}: a SampleRef in one run, the run a link (checked against the store later). */
    private static void occurrence(Object value, String what) {
        final Map occurrence = fields(value, OCCURRENCE_KEYS, what)
        shortText(occurrence.get('pipeline'), "${what}.pipeline")
        shortText(occurrence.get('id'), "${what}.id")
        if( !(occurrence.get('run') instanceof Cid) )
            throw invalid("${what}.run is a link to a RunCompletion", '/value')
    }

    /** The value as a map with exactly {@code keys}. */
    private static Map fields(Object value, Set<String> keys, String what) {
        final List<String> wanted = keys.toList()
        if( !(value instanceof Map) )
            throw invalid("${what} is a map of ${wanted.join(', ')}", '/value')
        final Map map = (Map) value
        if( !((Object) keys).equals(map.keySet()) )
            throw invalid("${what} has exactly the fields ${wanted.join(', ')}; got ${map.keySet().join(', ') ?: 'none'}", '/value')
        return map
    }

    private static void shortText(Object value, String what) {
        if( !(value instanceof String) || !((String) value) || ((String) value).length() > MAX_SAMPLE_CHARS )
            throw invalid("${what} is a non-empty string of at most ${MAX_SAMPLE_CHARS} characters", '/value')
    }

    private static String render(Object value) {
        return value instanceof String ? "'${value}'".toString() : String.valueOf(value)
    }

    // -------------------------------------------------------------------- Project

    /** P4 plan 1.1: the server fills schema and asserted_by; created_at is the request's, so a retry is the same block. */
    private Draft projectDraft(Map map) {
        unknownKeys(map, PROJECT_KEYS, '')
        final Object name = map.get('name')
        if( !(name instanceof String && ((String) name).trim() && ((String) name).length() <= Project.MAX_NAME_CHARS) )
            throw invalid("name is a non-blank string of at most ${Project.MAX_NAME_CHARS} characters", '/name')
        final Object description = map.get('description')
        if( !(description instanceof String) || ((String) description).length() > Project.MAX_DESCRIPTION_CHARS )
            throw invalid("description is a string of at most ${Project.MAX_DESCRIPTION_CHARS} characters, empty allowed", '/description')
        final Object createdAt = map.get('created_at')
        final long createdMillis = instantOf(createdAt, 'created_at')
        final Closure<Map<String, Object>> sign = { String who ->
            new Project(who, (String) name, (String) description, (String) createdAt).toCbor()
        } as Closure<Map<String, Object>>
        return new Draft(sign, StoreLogKind.PROJECT, '/description', { -> },
            { -> checkClock((String) createdAt, createdMillis, 'created_at') })
    }

    // ------------------------------------------------------------------ plumbing

    /** A request's ISO-8601 UTC instant with milliseconds, as epoch millis; the field is also its pointer. */
    private static long instantOf(Object value, String field) {
        if( !(value instanceof String) || !TIMESTAMP.matcher((String) value).matches() )
            throw invalid("${field} is ISO-8601 UTC with milliseconds, e.g. 2026-09-25T10:00:00.000Z", "/${field}".toString())
        try {
            return Instant.parse((String) value).toEpochMilli()
        }
        catch( DateTimeParseException e ) {
            throw invalid("${field} '${value}' is not a real instant", "/${field}".toString())
        }
    }

    private void checkClock(String text, long millis, String field) {
        final long now = clock.call()
        if( Math.abs(now - millis) > CLOCK_SKEW_MILLIS )
            throw new PutError(PutError.CLOCK_SKEW, "${field} ${text} is more than 10 minutes from this server's clock (${Index.isoMillis(now)})", "/${field}".toString())
    }

    /**
     * The dry run's "exists": advice for the page, so a member that cannot be
     * read is warned about and skipped rather than blocking a write to the
     * writable member.
     */
    private boolean heldAnywhere(Cid address) {
        if( !(store instanceof CompositeStore) )
            return store.has(address)
        return ((CompositeStore) store).hasInReadable(address) { BlockStore member, Exception e ->
            log.warn("store member '${member.alias()}' could not be read, so the dry run does not count it: ${e.message}")
        }
    }

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
        if( existing != null )
            return existing.name
        final String name = append(address, kind)
        ingest()
        return name
    }

    private String append(Cid address, StoreLogKind kind) {
        final long now = clock.call()
        StoreLog.append(writable, kind, address, now)
        return StoreLog.entryName(kind, address, now)
    }

    /** The write is done; the index is derived, so a failure here only warns (DESIGN.md §0 rule 3). */
    private void ingest() {
        try {
            index.catchUp(writable, StoreLog.of(writable), writable.alias(), store)
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
