package robsyme.cas.core

import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.nio.file.AccessDeniedException
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.function.Supplier
import java.util.regex.Pattern

import groovy.json.JsonException
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * The builder behind `POST /api/handoff` (explorer v0.3 P4 plan 1.5, ruling
 * R5). A request is plain JSON: the generated files as text, the downstream
 * params, the Selection handed off and, optionally, its Project. Each file
 * becomes a raw block, and the Handoff block roots them with the params, every
 * file reference as `cas://<cid>/<name>` and every value through the
 * RunManifest params scrub ({@link Records#scrub}). The response gives the
 * params resolved and unscrubbed; they are never stored.
 *
 * Order, as {@link Put}'s: shape and size on a probe signed by a
 * one-character name; then, for a write, writable and the Selection and
 * Project held; only then is the signer asked and the block built; then dry
 * run, "already written" (success, unchanged), the clock, the raw blocks,
 * the Handoff, its `handoff` Store Log entry last, and ingest. A request
 * the store refuses never asks who signs.
 */
@Slf4j
@CompileStatic
class HandoffWriter {

    static final String BAD_REQUEST = 'bad_request'
    static final int MAX_TEMPLATE_CHARS = 256

    /** As Put's (private there): ISO-8601 UTC with milliseconds. */
    private static final Pattern TIMESTAMP = ~/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/
    private static final Pattern FILE_NAME = ~/^[A-Za-z0-9._-]{1,128}$/
    /** A pipeline name such as nf-core/differentialabundance; the block keeps it, so no free text. */
    private static final Pattern TEMPLATE = ~/^[A-Za-z0-9._\/-]{1,256}$/
    private static final Set<String> KEYS = ['template', 'selection', 'project', 'files', 'params', 'timestamp'] as Set
    private static final Set<String> FILE_KEYS = ['name', 'text'] as Set

    /** A refused hand-off: plain JSON `{error, message}`, 409 for not_writable, else 400. */
    @CompileStatic
    static class Refusal extends RuntimeException {
        final String code

        Refusal(String code, String message) {
            super(message)
            this.code = code
        }

        int getStatus() { code == PutError.NOT_WRITABLE ? 409 : 400 }

        byte[] body() {
            final Map<String, Object> out = new LinkedHashMap<String, Object>()
            out.put('error', code)
            out.put('message', message)
            return JsonOutput.toJson(out).getBytes(StandardCharsets.UTF_8)
        }
    }

    /** What a hand-off answered. {@code entry} is null for a dry run. */
    @CompileStatic
    static class Result {
        final Cid address
        /** Sorted by name, as the block holds them. */
        final List<Leaf> files
        /** The request's params with file references resolved, not scrubbed. */
        final Map<String, String> params
        final String entry
        final boolean written

        Result(Cid address, List<Leaf> files, Map<String, String> params, String entry, boolean written) {
            this.address = address
            this.files = files
            this.params = params
            this.entry = entry
            this.written = written
        }

        byte[] body() {
            final Map<String, Object> out = new LinkedHashMap<String, Object>()
            out.put('address', address.toString())
            out.put('files', files.collect { Leaf f -> [name: f.name, address: f.address.toString(), size: f.size] as Map<String, Object> })
            out.put('params', params)
            out.put('entry', entry)
            out.put('written', written)
            return JsonOutput.toJson(out).getBytes(StandardCharsets.UTF_8)
        }
    }

    /** A request turned into a block once someone signs it, with the files to write and what to check first. */
    @CompileStatic
    private static final class Draft {
        /** The Handoff, signed by the name it is given. */
        Closure<Handoff> sign
        Cid selection
        Cid project
        /** Each file's bytes, by name. */
        Map<String, byte[]> contents
        Map<String, String> params
        long timestampMillis
    }

    /** A signed Handoff and its encoding. */
    @CompileStatic
    private static final class Signed {
        Handoff handoff
        byte[] bytes
        Cid address
    }

    /** As Put's: the shortest name a block takes, so a probe never encodes larger than the signed block. */
    private static final String PROBE_SIGNER = 'x'

    private final BlockStore store
    private final BlockStore writable
    private final Index index
    /** Who signs, asked only once the request is well formed. */
    private final Supplier<String> assertedBy
    private final Closure<Long> clock
    private final Closure catchUp
    /** The monitor writes hold: explore passes its Put, so a hand-off, a put and an export never overlap on the index. */
    private final Object lock

    HandoffWriter(BlockStore store, BlockStore writable, Index index, Supplier<String> assertedBy, Closure<Long> clock, Closure catchUp) {
        this(store, writable, index, assertedBy, clock, catchUp, null)
    }

    HandoffWriter(BlockStore store, BlockStore writable, Index index, Supplier<String> assertedBy, Closure<Long> clock,
                  Closure catchUp, Object lock) {
        this.store = store
        this.writable = writable
        this.index = index
        this.assertedBy = assertedBy
        this.clock = clock
        this.catchUp = catchUp
        this.lock = lock ?: this
    }

    /** Decodes the body (UTF-8, plain JSON) outside the lock; see {@link #handoff(Object, boolean)}. */
    Result handoff(byte[] body, boolean dryRun) {
        if( body.length > Put.MAX_REQUEST_BYTES )
            throw new Refusal(PutError.TOO_LARGE, "the request is ${body.length} bytes, over ${Put.MAX_REQUEST_BYTES}")
        final String text
        try {
            text = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(body)).toString()
        }
        catch( CharacterCodingException e ) {
            throw bad('the request is not UTF-8')
        }
        final Object request
        try {
            request = new JsonSlurper().parseText(text)
        }
        catch( JsonException | IllegalArgumentException | StackOverflowError e ) {
            throw bad("the request is not JSON: ${e.message ?: e.class.simpleName}")
        }
        return handoff(request, dryRun)
    }

    /**
     * Signed outside the lock, as Put signs: signing asks {@code assertedBy},
     * which can wait on the first identity resolution. Before that the shape
     * and size are checked on a probe, and a write's store-level checks run,
     * so a request they refuse never asks who signs. Everything that reads the
     * store or the index runs under the lock.
     */
    Result handoff(Object request, boolean dryRun) {
        final Draft draft = draftOf(request)
        signed(draft, PROBE_SIGNER)
        if( !dryRun ) {
            requireWritable()
            synchronized( lock ) {
                held(draft.selection, 'selection', Records.SELECTION)
                if( draft.project != null )
                    held(draft.project, 'project', Records.PROJECT)
            }
        }
        final Signed block = signed(draft, assertedBy.get())
        synchronized( lock ) {
            return write(draft, block, dryRun)
        }
    }

    /** The Handoff signed by {@code who}, encoded; too_large over the block cap. */
    private static Signed signed(Draft draft, String who) {
        final Handoff handoff = draft.sign.call(who)
        final byte[] bytes = DagCbor.encode(handoff.toCbor())
        if( bytes.length > Put.MAX_BLOCK_BYTES )
            throw new Refusal(PutError.TOO_LARGE, "the encoded Handoff is ${bytes.length} bytes, over ${Put.MAX_BLOCK_BYTES}; its params are too long")
        return new Signed(handoff: handoff, bytes: bytes, address: DagCbor.cidOf(bytes))
    }

    private void requireWritable() {
        if( !writable.isWritable() )
            throw new Refusal(PutError.NOT_WRITABLE, "store member '${writable.alias()}' is not writable")
    }

    private Result write(Draft draft, Signed block, boolean dryRun) {
        catchUp.call()
        if( dryRun )
            return new Result(block.address, block.handoff.files, draft.params, null, false)
        if( writable.has(block.address) )
            return new Result(block.address, block.handoff.files, draft.params, entryFor(block.address), false)
        validate(draft)
        try {
            for( Leaf f : block.handoff.files ) {
                if( writable.has(f.address) )
                    continue
                final byte[] bytes = draft.contents.get(f.name)
                writable.put(f.address, new ByteArrayInputStream(bytes), (long) bytes.length)
            }
            writable.put(block.address, new ByteArrayInputStream(block.bytes), (long) block.bytes.length)
        }
        catch( AccessDeniedException e ) {
            throw new Refusal(PutError.NOT_WRITABLE, "store member '${writable.alias()}' refused the write: ${e.message}")
        }
        final String entry = append(block.address)
        ingest()
        return new Result(block.address, block.handoff.files, draft.params, entry, true)
    }

    /** The clock: after "already written", so a retried hand-off dedupes first, as a Claim's timestamp does. */
    private void validate(Draft draft) {
        final long now = clock.call()
        if( Math.abs(now - draft.timestampMillis) > Put.CLOCK_SKEW_MILLIS )
            throw new Refusal(PutError.CLOCK_SKEW, "timestamp ${Index.isoMillis(draft.timestampMillis)} is more than 10 minutes from this server's clock (${Index.isoMillis(now)})")
    }

    /** The composition holds {@code cid} and it is a block of {@code kind}: not_found or wrong_kind otherwise. */
    private void held(Cid cid, String field, String kind) {
        if( !store.has(cid) )
            throw new Refusal(PutError.NOT_FOUND, "${field} ${cid} is not in any member of this composition")
        String found = null
        if( !cid.isRaw() ) {
            final InputStream input = store.open(cid)
            try {
                final Object block = DagCbor.decode(input.readAllBytes())
                found = block instanceof Map ? Records.kindOf((Map) block) : null
            }
            finally {
                input.close()
            }
        }
        if( found != kind )
            throw new Refusal(PutError.WRONG_KIND, "${field} ${cid} is ${found == null ? 'not a metadata block' : "a ${found}"}, not a ${kind}")
    }

    // ---------------------------------------------------------------- the draft

    private Draft draftOf(Object request) {
        if( !(request instanceof Map) )
            throw bad('the request is a JSON object with template, selection, project, files, params and timestamp')
        final Map map = (Map) request
        for( Object key : map.keySet() )
            if( !KEYS.contains(key) )
                throw bad("unknown field '${key}'; a hand-off takes ${KEYS.join(', ')}")
        final Object template = map.get('template')
        if( !(template instanceof String) || !TEMPLATE.matcher((String) template).matches() )
            throw bad("template is 1 to ${MAX_TEMPLATE_CHARS} characters from A-Z a-z 0-9 . _ / -, such as nf-core/differentialabundance")
        final Cid selection = cidOf(map.get('selection'), 'selection')
        if( selection == null )
            throw bad('selection is the cid of a Selection')
        final Cid project = cidOf(map.get('project'), 'project')

        final Map<String, byte[]> contents = new LinkedHashMap<String, byte[]>()
        final List<Leaf> leaves = new ArrayList<Leaf>()
        final Object files = map.get('files')
        if( !(files instanceof List) || ((List) files).isEmpty() )
            throw bad('files is a list of at least one {"name", "text"}')
        final List list = (List) files
        for( int i = 0; i < list.size(); i++ ) {
            final String at = "files[${i}]".toString()
            if( !(list[i] instanceof Map) )
                throw bad("${at} is {\"name\", \"text\"}")
            final Map file = (Map) list[i]
            for( Object key : file.keySet() )
                if( !FILE_KEYS.contains(key) )
                    throw bad("${at} has an unknown field '${key}'; a file takes name and text")
            final Object name = file.get('name')
            if( !(name instanceof String) || !FILE_NAME.matcher((String) name).matches() || name in ['.', '..'] )
                throw bad("${at}.name is a base name of 1 to 128 characters from A-Z a-z 0-9 . _ - (not . or ..), got ${name == null ? 'null' : "'${name}'"}")
            if( contents.containsKey(name) )
                throw bad("${at}.name '${name}' names a file already given")
            final Object text = file.get('text')
            if( !(text instanceof String) )
                throw bad("${at}.text is a string")
            if( !wellFormed((String) text) )
                throw bad("${at}.text holds a lone surrogate, which has no UTF-8 form")
            final byte[] bytes = ((String) text).getBytes(StandardCharsets.UTF_8)
            contents.put((String) name, bytes)
            leaves.add(Leaf.of((String) name, Cid.of(Cid.RAW, Hashing.sha256(bytes)), (long) bytes.length))
        }
        final Map<String, Leaf> byName = new HashMap<String, Leaf>()
        for( Leaf f : leaves )
            byName.put(f.name, f)

        final Object params = map.get('params')
        if( !(params instanceof Map) )
            throw bad('params is a JSON object of strings and {"file": <name>} references')
        final Map<String, String> resolved = new LinkedHashMap<String, String>()
        for( Map.Entry e : ((Map) params).entrySet() ) {
            final String key = (String) e.key
            final String at = "params.${key}".toString()
            if( !key || !wellFormed(key) )
                throw bad("params has an empty or malformed name '${key}'")
            final Object value = e.value
            if( value instanceof String ) {
                if( !wellFormed((String) value) )
                    throw bad("${at} holds a lone surrogate")
                resolved.put(key, (String) value)
                continue
            }
            if( value instanceof Map && ((Map) value).keySet() == (['file'] as Set) && ((Map) value).get('file') instanceof String ) {
                final Leaf f = byName.get((String) ((Map) value).get('file'))
                if( f == null )
                    throw bad("${at} names the file '${((Map) value).get('file')}', which is not one of files")
                resolved.put(key, "cas://${f.address}/${f.name}".toString())
                continue
            }
            throw bad("${at} is a string or {\"file\": <name>}")
        }

        final Object timestamp = map.get('timestamp')
        if( !(timestamp instanceof String) || !TIMESTAMP.matcher((String) timestamp).matches() )
            throw bad('timestamp is ISO-8601 UTC with milliseconds, e.g. 2026-10-25T10:00:00.000Z')
        final long timestampMillis
        try {
            timestampMillis = Instant.parse((String) timestamp).toEpochMilli()
        }
        catch( DateTimeParseException e ) {
            throw bad("timestamp '${timestamp}' is not a real instant")
        }

        // The block's params are scrubbed as a RunManifest's are (ruling R5).
        final Map<String, String> scrubbed = new LinkedHashMap<String, String>()
        for( Map.Entry e : ((Map) Records.scrub(resolved)).entrySet() )
            scrubbed.put((String) e.key, (String) e.value)
        final Closure<Handoff> sign = { String who ->
            new Handoff(who, (String) template, selection, project, leaves, scrubbed, (String) timestamp)
        }
        return new Draft(sign: sign, selection: selection, project: project, contents: contents,
            params: resolved, timestampMillis: timestampMillis)
    }

    /** A cid as text, or as a DAG-JSON link {"/": text}; null for null. */
    private static Cid cidOf(Object value, String field) {
        if( value == null )
            return null
        Object text = value
        if( value instanceof Map && ((Map) value).size() == 1 && ((Map) value).containsKey('/') )
            text = ((Map) value).get('/')
        if( text instanceof String ) {
            try {
                return Cid.parse((String) text)
            }
            catch( IllegalArgumentException e ) {
                throw bad("${field} is not a cid: ${e.message}")
            }
        }
        throw bad("${field} is a cid string or a DAG-JSON link {\"/\": <cid>}")
    }

    /** False for a string holding a lone surrogate, which has no UTF-8 form (JSON's \\ud800 can write one). */
    private static boolean wellFormed(String s) {
        final int n = s.length()
        for( int i = 0; i < n; i++ ) {
            final char c = s.charAt(i)
            if( Character.isHighSurrogate(c) ) {
                if( i + 1 >= n || !Character.isLowSurrogate(s.charAt(i + 1)) )
                    return false
                i++
            }
            else if( Character.isLowSurrogate(c) )
                return false
        }
        return true
    }

    private static Refusal bad(String message) {
        return new Refusal(BAD_REQUEST, message)
    }

    // ------------------------------------------------------------------ plumbing
    // entryFor, append and ingest are Put's (private there), copied.

    private String entryFor(Cid address) {
        final StoreLogEntry existing = index.firstLogEntry(address, writable.alias())
        if( existing != null )
            return existing.name
        final String name = append(address)
        ingest()
        return name
    }

    private String append(Cid address) {
        final long now = clock.call()
        StoreLog.append(writable, StoreLogKind.HANDOFF, address, now)
        return StoreLog.entryName(StoreLogKind.HANDOFF, address, now)
    }

    /** The write is done; the index is derived, so a failure here only warns (DESIGN.md §0 rule 3). */
    private void ingest() {
        try {
            index.catchUp(writable, StoreLog.of(writable), writable.alias(), store)
        }
        catch( Exception e ) {
            log.warn("the index could not ingest a hand-off just written; it is derived and catches up later: ${e.message}")
        }
    }
}
