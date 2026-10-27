package robsyme.cas.explore

import java.security.MessageDigest
import java.security.SecureRandom
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Supplier
import java.util.regex.Matcher
import java.util.regex.Pattern

import com.sun.net.httpserver.Headers
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import groovy.json.JsonException
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import robsyme.cas.CopyErrors
import robsyme.cas.CopyService
import robsyme.cas.Principal
import robsyme.cas.core.Cid
import robsyme.cas.core.HandoffWriter
import robsyme.cas.core.Multibase
import robsyme.cas.core.Put
import robsyme.cas.core.PutError

/**
 * The explorer's loopback server (DESIGN.md §15, block explorer spec sections
 * 2 and 6): the page, and for every member only its snapshot, its blocks and
 * its Store Log listing, with Range. One user, one machine: it binds loopback,
 * answers only to its own origin, and sends no CORS headers.
 */
@Slf4j
@CompileStatic
class ExploreServer {

    /** The samplesheet export of a Selection (decision 1 of the milestone 2 plan). */
    static interface Exporter {
        byte[] samplesheet(Cid selection, String format)
    }

    /**
     * One copy for `POST /api/copy` (P3 plan 1.7): the request as its JSON keys,
     * answered with CopyService's status and 1.3 body. It may throw; the
     * {@code throws} keeps a closure's checked exception from arriving
     * wrapped in an UndeclaredThrowableException.
     */
    static interface Copier {
        CopyService.Outcome copy(Map request, boolean dryRun) throws Exception
    }

    /** What members.json says about one member beyond its alias (plan 1.7). */
    static class MemberInfo {
        final String kind
        /** The S3 URI text of an S3 member; null for a local one, whose path is never exposed. */
        final String location
        final boolean copyTarget
        final String copyReason

        MemberInfo(String kind, String location, boolean copyTarget, String copyReason) {
            this.kind = kind
            this.location = location
            this.copyTarget = copyTarget
            this.copyReason = copyReason
        }
    }

    private static final Pattern MEMBER = ~/^\/m\/([a-z][a-z0-9_-]{0,31})\/(.*)$/
    private static final Pattern BLOCK = ~/^blocks\/([a-z2-7]{2})\/(b[a-z2-7]{58})$/
    private static final Pattern SNAPSHOT = ~/^index\/v\d{1,4}\.sqlite$/
    private static final Pattern LOG_ENTRY = ~/^\d{13}-[a-z]+-b[a-z2-7]{58}$/
    private static final Pattern SAMPLESHEET = ~/^\/api\/samplesheet\/(b[a-z2-7]{58})\.(csv|json)$/
    private static final byte[] NO_PAGE = ('<!doctype html><meta charset="utf-8"><title>nf-blocks</title>' +
        '<p>This build of nf-blocks carries no explorer page. Build it with <code>./gradlew assemble</code>.').getBytes('UTF-8')

    static final String TOKEN_HEADER = 'X-NF-Blocks-Token'
    private static final Set<String> JSON_TYPES = ['application/json', 'application/vnd.ipld.dag-json'] as Set
    private static final Set<String> COPY_TYPES = ['application/json'] as Set
    private static final SecureRandom RANDOM = new SecureRandom()

    private final LinkedHashMap<String, MemberFiles> members
    private final Supplier<Principal> principal
    private final String writableAlias
    private final byte[] page
    private final Put put
    private final String token
    private final Exporter exporter
    private Map<String, MemberInfo> memberInfo = Collections.emptyMap()
    private MemberProbe memberProbe
    private Copier copier
    private HandoffWriter handoffs
    private PrintStream err
    /** Held by a real copy for its whole run: one at a time per server (plan 1.7). Never Put's monitor. */
    private final ReentrantLock copying = new ReentrantLock()
    /**
     * Dry runs at once. Each walks a whole closure, so they are not free. One
     * more waits its turn, and only one (D10): a newer dry run takes the
     * waiting place, and the one it replaced answers 409 superseded.
     */
    static final int DRY_RUNS = 2
    private final Object dryRunTurns = new Object()
    private int dryRunsRunning
    /** The waiting dry run's ticket; null when none waits. */
    private Object dryRunWaiting
    /** Set by stop(): a waiting dry run answers 503 rather than holding stop() up. */
    private boolean stopping
    static final String SUPERSEDED = 'superseded'
    static final String STOPPING = 'stopping'
    private HttpServer server
    private ExecutorService executor

    /** How long stop() waits for an exchange in flight; a test seam. */
    @PackageScope int graceSeconds = 10
    /** How long stop() waits instead while a copy runs: a copy takes minutes, and is worth finishing. */
    @PackageScope int copyGraceSeconds = 30 * 60

    ExploreServer(LinkedHashMap<String, MemberFiles> members, String writableAlias, byte[] page) {
        this(members, writableAlias, page, null, null)
    }

    ExploreServer(LinkedHashMap<String, MemberFiles> members, String writableAlias, byte[] page, Put put, String token) {
        this(members, writableAlias, page, put, token, null)
    }

    ExploreServer(LinkedHashMap<String, MemberFiles> members, String writableAlias, byte[] page, Put put, String token, Exporter exporter) {
        this(members, writableAlias, page, put, token, exporter, null)
    }

    /**
     * {@code principal} answers who signs for this server; members.json tells the
     * page, and a null supplier (or a null answer) leaves the key out. It may block
     * while the identity resolves: members.json then waits for it, and every other
     * request is served meanwhile.
     */
    ExploreServer(LinkedHashMap<String, MemberFiles> members, String writableAlias, byte[] page, Put put, String token, Exporter exporter, Supplier<Principal> principal) {
        this.principal = principal
        this.members = members
        this.writableAlias = writableAlias
        this.page = page ?: NO_PAGE
        this.put = put
        this.token = token
        this.exporter = exporter
    }

    /**
     * Per-member facts for members.json. A member without an entry reads as
     * local and a copy target, with no location.
     */
    ExploreServer memberInfo(Map<String, MemberInfo> info) {
        this.memberInfo = info ?: Collections.<String, MemberInfo>emptyMap()
        return this
    }

    /**
     * Lets members.json grey out an S3 copy target that cannot be reached now
     * (expired credentials, no network), with a reason that says what to do.
     * Asked only while this server can copy.
     */
    ExploreServer memberProbe(MemberProbe probe) {
        this.memberProbe = probe
        return this
    }

    /**
     * Lets this server run copies through {@link CopyService} over the loaded
     * config, signed with this server's principal (the one members.json
     * shows). {@code err} takes each copy's notices, and stop()'s word about a
     * copy in flight.
     */
    ExploreServer copies(Map config, PrintStream err) {
        return copies({ Map request, boolean dryRun -> CopyService.execute(config, request, dryRun, err, principal) } as Copier, err)
    }

    ExploreServer copies(Copier copier, PrintStream err) {
        this.copier = copier
        this.err = err
        return this
    }

    /** Lets this server take `POST /api/handoff` (P4 plan 1.5), behind the token. */
    ExploreServer handoffs(HandoffWriter writer) {
        this.handoffs = writer
        return this
    }

    /** 16 random bytes as base32: what the printed URL carries (decision 12). */
    static String newToken() {
        final byte[] bytes = new byte[16]
        RANDOM.nextBytes(bytes)
        return Multibase.base32Encode(bytes)
    }

    ExploreServer start(int port) {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0)
        executor = Executors.newFixedThreadPool(8, { Runnable r ->
            final Thread t = new Thread(r, 'nf-blocks-explore')
            t.daemon = true
            return t
        } as ThreadFactory)
        server.executor = executor
        server.createContext('/', { HttpExchange exchange -> handle(exchange) } as HttpHandler)
        server.start()
        return this
    }

    int getPort() { server.address.port }

    String getUrl() { "http://127.0.0.1:${port}/" }

    String getLaunchUrl() { token ? "${url}?token=${token}".toString() : url }

    /**
     * Stops accepting new exchanges and waits for exchanges already in flight
     * to finish before the executor's threads are touched, so a write or an
     * export under way (both hold {@code Put}'s monitor) completes rather
     * than being interrupted mid-write (final review finding 5). Only a
     * request that outlives the grace period is cut off. A real copy in
     * flight gets the longer copy grace: stop() says on stderr that it waits
     * for it, and says again if the copy is cut off all the same. The lock is
     * asked again once the server has stopped, so a copy that took it
     * meanwhile gets the copy grace too.
     */
    void stop() {
        synchronized( dryRunTurns ) {
            stopping = true
            dryRunTurns.notifyAll()
        }
        boolean told = false
        if( copying.isLocked() ) {
            waitNotice()
            told = true
        }
        server?.stop(told ? copyGraceSeconds : graceSeconds)
        if( executor != null ) {
            executor.shutdown()
            final boolean copy = copying.isLocked()
            if( copy && !told )
                waitNotice()
            try {
                if( !executor.awaitTermination(copy ? copyGraceSeconds : graceSeconds, TimeUnit.SECONDS) ) {
                    if( copying.isLocked() )
                        notice('nf-blocks:explore: the copy in flight was cut off; the destination has no Transfer for it. Run the copy again to finish it')
                    executor.shutdownNow()
                }
            }
            catch( InterruptedException e ) {
                Thread.currentThread().interrupt()
                executor.shutdownNow()
            }
        }
    }

    private void waitNotice() {
        notice("nf-blocks:explore: a copy is running; waiting up to ${copyGraceSeconds} s for it to finish before stopping. " +
            "To stop now, kill -9 ${ProcessHandle.current().pid()}: the destination keeps the blocks and entries already written " +
            'and gets no Transfer; running the copy again finishes it')
    }

    private void notice(String line) {
        if( err != null ) {
            err.println(line)
            err.flush()
        }
        else
            log.warn(line)
    }

    private void handle(HttpExchange exchange) {
        try {
            if( !ownOrigin(exchange) ) {
                text(exchange, 403, 'refused: this server answers only to its own origin')
                return
            }
            if( exchange.requestMethod == 'POST' ) {
                if( exchange.requestURI.rawPath == '/api/put' )
                    write(exchange)
                else if( exchange.requestURI.rawPath == '/api/copy' )
                    copy(exchange)
                else if( exchange.requestURI.rawPath == '/api/handoff' )
                    handoff(exchange)
                else {
                    exchange.responseHeaders.set('Allow', 'GET, HEAD')
                    text(exchange, 405, 'only /api/put, /api/copy and /api/handoff take a POST')
                }
                return
            }
            if( !(exchange.requestMethod in ['GET', 'HEAD']) ) {
                exchange.responseHeaders.set('Allow', 'GET, HEAD')
                text(exchange, 405, 'the explorer server is read-only')
                return
            }
            // The raw path, so an encoded `..` is never decoded into a traversal.
            route(exchange, exchange.requestURI.rawPath)
        }
        catch( Exception e ) {
            // The raw path only (final review finding 8): the full request URI
            // may carry a query string, and this endpoint's only query
            // parameter is dry_run, but a client could still send ?token=...
            // by mistake (the token belongs in a header, never a query).
            final String failed = "explore: ${exchange.requestMethod} ${exchange.requestURI.rawPath} failed: ${e.message}"
            log.warn(failed, e)
            // A plugin verb keeps no log file, so the page's "internal error"
            // would otherwise leave nothing to read.
            if( err != null ) {
                err.println("nf-blocks:${failed}")
                err.flush()
            }
            try {
                text(exchange, 500, 'internal error')
            }
            catch( Exception ignored ) {
                // The response had already started.
            }
        }
        finally {
            exchange.close()
        }
    }

    /** Host must be this server, and Origin, when sent, too (spec section 9.5): the DNS-rebinding guard. */
    private boolean ownOrigin(HttpExchange exchange) {
        final int port = getPort()
        final String host = exchange.requestHeaders.getFirst('Host')
        if( !(host in ["127.0.0.1:${port}".toString(), "localhost:${port}".toString()]) )
            return false
        final String origin = exchange.requestHeaders.getFirst('Origin')
        return origin == null || origin in ["http://127.0.0.1:${port}".toString(), "http://localhost:${port}".toString()]
    }

    private void route(HttpExchange exchange, String path) {
        if( path == '/' || path == '/index.html' ) {
            bytes(exchange, 200, 'text/html; charset=utf-8', page)
            return
        }
        if( path == '/members.json' ) {
            bytes(exchange, 200, 'application/json', membersJson())
            return
        }
        final Matcher sheet = SAMPLESHEET.matcher(path)
        if( sheet.matches() && exporter != null ) {
            samplesheet(exchange, Cid.parse(sheet.group(1)), sheet.group(2))
            return
        }
        final Matcher member = MEMBER.matcher(path)
        if( !member.matches() || !members.containsKey(member.group(1)) ) {
            text(exchange, 404, 'not found')
            return
        }
        final MemberFiles files = members.get(member.group(1))
        final String rel = member.group(2)
        if( rel == 'log/' ) {
            bytes(exchange, 200, 'application/json', logJson(files))
            return
        }
        final Matcher block = BLOCK.matcher(rel)
        if( block.matches() && block.group(2).endsWith(block.group(1)) ) {
            file(exchange, files, rel, 'application/octet-stream', 'public, max-age=31536000, immutable')
            return
        }
        if( SNAPSHOT.matcher(rel).matches() ) {
            file(exchange, files, rel, 'application/vnd.sqlite3', 'no-cache')
            return
        }
        text(exchange, 404, 'not found')
    }

    /** The S3 copy targets that cannot be reached now, with why; empty when this server cannot copy. */
    private Map<String, String> unreachableTargets() {
        if( memberProbe == null || !canCopy() )
            return Collections.<String, String>emptyMap()
        final List<String> targets = members.keySet().findAll { String alias ->
            final MemberInfo info = memberInfo.get(alias)
            info != null && info.kind == 's3' && info.copyTarget
        } as List<String>
        return targets ? memberProbe.unreachable(targets) : Collections.<String, String>emptyMap()
    }

    private byte[] membersJson() {
        final Map<String, String> unreachable = unreachableTargets()
        final List<Map> list = members.keySet().collect { String alias ->
            final MemberInfo info = memberInfo.get(alias) ?: new MemberInfo('local', null, true, null)
            final String unreached = unreachable.get(alias)
            [alias: alias, writable: alias == writableAlias, base: "m/${alias}/".toString(),
             kind: info.kind, location: info.location,
             copy_target: info.copyTarget && unreached == null, copy_reason: unreached ?: info.copyReason] as Map
        }
        final Map<String, Object> body = [members: list, write: put != null] as Map<String, Object>
        final Principal signer = principal?.get()
        if( signer != null )
            body.put('identity', [principal: signer.name, source: signer.source])
        body.put('copy', canCopy())
        return JsonOutput.toJson(body).getBytes('UTF-8')
    }

    /** members.json `copy`: the endpoint is here, behind the token. */
    private boolean canCopy() {
        return copier != null && token != null
    }

    /** POST /api/put (spec sections 9.2 and 9.5): token, content type and size, then the builder. */
    private void write(HttpExchange exchange) {
        if( put == null || token == null ) {
            text(exchange, 405, 'this server was started without a writable member')
            return
        }
        final byte[] body = admitted(exchange, JSON_TYPES, 'application/json or application/vnd.ipld.dag-json')
        if( body == null )
            return
        try {
            dagJson(exchange, 200, put.put(body, dryRun(exchange)).body())
        }
        catch( PutError e ) {
            dagJson(exchange, e.status, e.body())
        }
    }

    /**
     * POST /api/handoff (P4 plan 1.5): the transport checks of /api/put, with
     * plain JSON only, then the Handoff writer. Its answer and its refusals
     * are plain JSON.
     */
    private void handoff(HttpExchange exchange) {
        if( handoffs == null || token == null ) {
            text(exchange, 405, 'this server was started without a writable member')
            return
        }
        final byte[] body = admitted(exchange, COPY_TYPES, 'application/json')
        if( body == null )
            return
        try {
            bytes(exchange, 200, 'application/json', handoffs.handoff(body, dryRun(exchange)).body())
        }
        catch( HandoffWriter.Refusal e ) {
            bytes(exchange, e.status, 'application/json', e.body())
        }
    }

    /**
     * POST /api/copy (P3 plan 1.7): the transport checks of /api/put, then one
     * CopyService run, synchronous. A real copy holds this server's copy lock
     * for its whole run, and a second one meanwhile is 409 copy_running. A dry
     * run takes no lock: it writes no block, entry or snapshot. It takes one of
     * {@link #DRY_RUNS} turns, or waits for one; a newer dry run replaces the
     * one waiting, which answers 409 superseded (D10), so the page's re-plans
     * never queue up behind each other and hold the server's threads. Put's
     * monitor is never taken, so writes and exports go on while a copy runs.
     */
    private void copy(HttpExchange exchange) {
        if( !canCopy() ) {
            text(exchange, 405, 'this server was started without copying')
            return
        }
        final byte[] body = admitted(exchange, COPY_TYPES, 'application/json')
        if( body == null )
            return
        final Object request
        try {
            request = parseJson(body)
        }
        catch( JsonException | IllegalArgumentException | StackOverflowError e ) {
            json(exchange, 400, error(CopyService.BAD_REQUEST_CODE, "the body is not JSON: ${e.message ?: e.class.simpleName}".toString()))
            return
        }
        if( !(request instanceof Map) ) {
            json(exchange, 400, error(CopyService.BAD_REQUEST_CODE, 'the body is a JSON object with root, from, to, depth, claims and accept_audit'))
            return
        }
        if( dryRun(exchange) ) {
            final String refused = dryRunTurn()
            if( refused == SUPERSEDED ) {
                json(exchange, 409, error(SUPERSEDED, 'a newer dry run replaced this one while it waited'))
                return
            }
            if( refused == STOPPING ) {
                json(exchange, 503, error(STOPPING, 'the server is stopping'))
                return
            }
            try {
                runCopy(exchange, (Map) request, true)
            }
            finally {
                synchronized( dryRunTurns ) {
                    dryRunsRunning--
                    dryRunTurns.notifyAll()
                }
            }
            return
        }
        if( !copying.tryLock() ) {
            json(exchange, 409, error('copy_running', 'another copy is running on this server; try again when it has finished'))
            return
        }
        try {
            runCopy(exchange, (Map) request, false)
        }
        finally {
            copying.unlock()
        }
    }

    /**
     * A dry run's turn (D10): null once it holds one, which the caller gives
     * back; else why it must not run. It waits, if it must, in the one
     * waiting place, taken from any dry run already there; it leaves when a
     * newer one takes the place (SUPERSEDED) or when stop() begins, or its
     * thread is interrupted (STOPPING).
     */
    private String dryRunTurn() {
        final Object ticket = new Object()
        synchronized( dryRunTurns ) {
            if( stopping )
                return STOPPING
            if( dryRunWaiting == null && dryRunsRunning < DRY_RUNS ) {
                dryRunsRunning++
                return null
            }
            dryRunWaiting = ticket
            dryRunTurns.notifyAll()
            while( true ) {
                if( !dryRunWaiting.is(ticket) )
                    return SUPERSEDED
                if( stopping ) {
                    dryRunWaiting = null
                    return STOPPING
                }
                if( dryRunsRunning < DRY_RUNS ) {
                    dryRunWaiting = null
                    dryRunsRunning++
                    return null
                }
                try {
                    dryRunTurns.wait()
                }
                catch( InterruptedException e ) {
                    if( dryRunWaiting.is(ticket) )
                        dryRunWaiting = null
                    Thread.currentThread().interrupt()
                    return STOPPING
                }
            }
        }
    }

    /** CopyService's status and body as JSON; anything it throws is 500 copy_failed. */
    private void runCopy(HttpExchange exchange, Map request, boolean dryRun) {
        final CopyService.Outcome outcome
        try {
            outcome = copier.copy(request, dryRun)
        }
        catch( Exception e ) {
            log.warn("explore: ${dryRun ? 'dry run of a copy' : 'copy'} of ${request.get('root')} failed: ${e.message}", e)
            // CopyService's own Failed carries its message redacted; any other exception is told by class, with paths redacted.
            json(exchange, 500, error('copy_failed', e instanceof CopyService.Failed ? e.message : CopyErrors.describe(e, Collections.<String, String> emptyMap())))
            return
        }
        json(exchange, outcome.status, outcome.body)
    }

    private static Object parseJson(byte[] body) {
        return new JsonSlurper().parseText(new String(body, 'UTF-8'))
    }

    private static Map<String, Object> error(String code, String message) {
        final Map<String, Object> body = new LinkedHashMap<String, Object>()
        body.put('error', code)
        body.put('message', message)
        return body
    }

    /**
     * The token, the content type and the size, each refused in plain text
     * (DESIGN.md §16): the body when all three pass, else null once the
     * refusal is sent.
     */
    private byte[] admitted(HttpExchange exchange, Set<String> types, String typesText) {
        final String given = exchange.requestHeaders.getFirst(TOKEN_HEADER)
        if( given == null || !MessageDigest.isEqual(given.getBytes('UTF-8'), token.getBytes('UTF-8')) ) {
            text(exchange, 403, 'refused: this needs the token in the URL nf-blocks:explore printed')
            return null
        }
        final String type = (exchange.requestHeaders.getFirst('Content-Type') ?: '').split(';')[0].trim().toLowerCase()
        if( !types.contains(type) ) {
            text(exchange, 415, "refused: send ${typesText}".toString())
            return null
        }
        final byte[] body = exchange.requestBody.readNBytes((int) Put.MAX_REQUEST_BYTES + 1)
        if( body.length > Put.MAX_REQUEST_BYTES ) {
            text(exchange, 413, "refused: the body is over ${Put.MAX_REQUEST_BYTES} bytes".toString())
            return null
        }
        return body
    }

    private static boolean dryRun(HttpExchange exchange) {
        return (exchange.requestURI.rawQuery ?: '').split('&').contains('dry_run=true')
    }

    private void samplesheet(HttpExchange exchange, Cid selection, String format) {
        final byte[] body
        try {
            body = exporter.samplesheet(selection, format)
        }
        catch( IllegalStateException | IllegalArgumentException e ) {
            text(exchange, 404, e.message)
            return
        }
        exchange.responseHeaders.set('Content-Disposition', "attachment; filename=\"selection-${selection.toString().take(16)}.${format}\"".toString())
        bytes(exchange, 200, format == 'csv' ? 'text/csv; charset=utf-8' : 'application/json', body)
    }

    private static void json(HttpExchange exchange, int status, Map body) {
        bytes(exchange, status, 'application/json', JsonOutput.toJson(body).getBytes('UTF-8'))
    }

    private static void dagJson(HttpExchange exchange, int status, byte[] body) {
        bytes(exchange, status, 'application/vnd.ipld.dag-json', body)
    }

    private static byte[] logJson(MemberFiles files) {
        final List<String> names = files.list('log').findAll { String n -> LOG_ENTRY.matcher(n).matches() }.sort()
        return JsonOutput.toJson([entries: names]).getBytes('UTF-8')
    }

    /**
     * The file opened once: its size, tag and bytes come from that one opening
     * (final review finding 1). The ETag lets the page notice a snapshot
     * rewritten under it (DESIGN.md §15).
     */
    private static void file(HttpExchange exchange, MemberFiles files, String rel, String type, String cache) {
        final MemberFiles.Opened opened = files.open(rel)
        if( opened == null ) {
            text(exchange, 404, 'not found')
            return
        }
        try {
            serve(exchange, opened, type, cache)
        }
        finally {
            opened.close()
        }
    }

    private static void serve(HttpExchange exchange, MemberFiles.Opened opened, String type, String cache) {
        final long size = opened.size
        final Headers headers = exchange.responseHeaders
        headers.set('Content-Type', type)
        headers.set('Accept-Ranges', 'bytes')
        headers.set('Cache-Control', cache)
        headers.set('X-Content-Type-Options', 'nosniff')
        if( opened.tag )
            headers.set('ETag', opened.tag)
        final ByteRange range
        try {
            range = ByteRange.parse(exchange.requestHeaders.getFirst('Range'), size)
        }
        catch( ByteRange.Unsatisfiable e ) {
            headers.set('Content-Range', "bytes */${size}".toString())
            exchange.sendResponseHeaders(416, -1)
            return
        }
        final long start = range != null ? range.start : 0L
        final long length = range != null ? range.length : size
        final int status = range != null ? 206 : 200
        if( range != null )
            headers.set('Content-Range', "bytes ${range.start}-${range.end}/${size}".toString())
        if( exchange.requestMethod == 'HEAD' || length == 0 ) {
            exchange.sendResponseHeaders(status, -1)
            return
        }
        // Opened before the headers go out, so a read that fails (an S3 object
        // replaced since HeadObject) is a 500, not a truncated 206.
        final InputStream input = opened.read(start, length)
        try {
            exchange.sendResponseHeaders(status, length)
            copy(input, exchange.responseBody, length)
        }
        finally {
            input.close()
        }
    }

    private static void copy(InputStream input, OutputStream output, long length) {
        final byte[] buffer = new byte[65536]
        long left = length
        while( left > 0 ) {
            final int n = input.read(buffer, 0, (int) Math.min((long) buffer.length, left))
            if( n < 0 )
                throw new IOException("the file ended ${left} bytes early")
            output.write(buffer, 0, n)
            left -= n
        }
    }

    private static void bytes(HttpExchange exchange, int status, String type, byte[] body) {
        exchange.responseHeaders.set('Content-Type', type)
        exchange.responseHeaders.set('Cache-Control', 'no-cache')
        exchange.responseHeaders.set('X-Content-Type-Options', 'nosniff')
        if( exchange.requestMethod == 'HEAD' ) {
            exchange.sendResponseHeaders(status, -1)
            return
        }
        exchange.sendResponseHeaders(status, body.length)
        exchange.responseBody.write(body)
    }

    private static void text(HttpExchange exchange, int status, String message) {
        bytes(exchange, status, 'text/plain; charset=utf-8', (message + '\n').getBytes('UTF-8'))
    }
}
