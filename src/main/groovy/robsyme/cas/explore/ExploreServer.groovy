package robsyme.cas.explore

import java.security.MessageDigest
import java.security.SecureRandom
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.regex.Matcher
import java.util.regex.Pattern

import com.sun.net.httpserver.Headers
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
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

    private static final Pattern MEMBER = ~/^\/m\/([a-z][a-z0-9_-]{0,31})\/(.*)$/
    private static final Pattern BLOCK = ~/^blocks\/([a-z2-7]{2})\/(b[a-z2-7]{58})$/
    private static final Pattern SNAPSHOT = ~/^index\/v\d{1,4}\.sqlite$/
    private static final Pattern LOG_ENTRY = ~/^\d{13}-[a-z]+-b[a-z2-7]{58}$/
    private static final byte[] NO_PAGE = ('<!doctype html><meta charset="utf-8"><title>nf-blocks</title>' +
        '<p>This build of nf-blocks carries no explorer page. Build it with <code>./gradlew assemble</code>.').getBytes('UTF-8')

    static final String TOKEN_HEADER = 'X-NF-Blocks-Token'
    private static final Set<String> JSON_TYPES = ['application/json', 'application/vnd.ipld.dag-json'] as Set
    private static final SecureRandom RANDOM = new SecureRandom()

    private final LinkedHashMap<String, MemberFiles> members
    private final String writableAlias
    private final byte[] page
    private final Put put
    private final String token
    private HttpServer server
    private ExecutorService executor

    ExploreServer(LinkedHashMap<String, MemberFiles> members, String writableAlias, byte[] page) {
        this(members, writableAlias, page, null, null)
    }

    ExploreServer(LinkedHashMap<String, MemberFiles> members, String writableAlias, byte[] page, Put put, String token) {
        this.members = members
        this.writableAlias = writableAlias
        this.page = page ?: NO_PAGE
        this.put = put
        this.token = token
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

    void stop() {
        server?.stop(0)
        executor?.shutdownNow()
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
                else {
                    exchange.responseHeaders.set('Allow', 'GET, HEAD')
                    text(exchange, 405, 'only /api/put takes a POST')
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
            log.warn("explore: ${exchange.requestMethod} ${exchange.requestURI} failed: ${e.message}", e)
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

    private byte[] membersJson() {
        final List<Map> list = members.keySet().collect { String alias ->
            [alias: alias, writable: alias == writableAlias, base: "m/${alias}/".toString()] as Map
        }
        return JsonOutput.toJson([members: list, write: put != null]).getBytes('UTF-8')
    }

    /** POST /api/put (spec sections 9.2 and 9.5): token, content type and size, then the builder. */
    private void write(HttpExchange exchange) {
        if( put == null || token == null ) {
            text(exchange, 405, 'this server was started without a writable member')
            return
        }
        final String given = exchange.requestHeaders.getFirst(TOKEN_HEADER)
        if( given == null || !MessageDigest.isEqual(given.getBytes('UTF-8'), token.getBytes('UTF-8')) ) {
            text(exchange, 403, 'refused: this needs the token in the URL nf-blocks:explore printed')
            return
        }
        final String type = (exchange.requestHeaders.getFirst('Content-Type') ?: '').split(';')[0].trim().toLowerCase()
        if( !JSON_TYPES.contains(type) ) {
            text(exchange, 415, 'refused: send application/json or application/vnd.ipld.dag-json')
            return
        }
        final byte[] body = exchange.requestBody.readNBytes((int) Put.MAX_REQUEST_BYTES + 1)
        if( body.length > Put.MAX_REQUEST_BYTES ) {
            text(exchange, 413, "refused: the body is over ${Put.MAX_REQUEST_BYTES} bytes")
            return
        }
        final boolean dryRun = (exchange.requestURI.rawQuery ?: '').split('&').contains('dry_run=true')
        try {
            dagJson(exchange, 200, put.put(body, dryRun).body())
        }
        catch( PutError e ) {
            dagJson(exchange, e.status, e.body())
        }
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
