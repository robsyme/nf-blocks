package robsyme.cas.explore

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

    private final LinkedHashMap<String, MemberFiles> members
    private final String writableAlias
    private final byte[] page
    private HttpServer server
    private ExecutorService executor

    ExploreServer(LinkedHashMap<String, MemberFiles> members, String writableAlias, byte[] page) {
        this.members = members
        this.writableAlias = writableAlias
        this.page = page ?: NO_PAGE
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
        return JsonOutput.toJson([members: list]).getBytes('UTF-8')
    }

    private static byte[] logJson(MemberFiles files) {
        final List<String> names = files.list('log').findAll { String n -> LOG_ENTRY.matcher(n).matches() }.sort()
        return JsonOutput.toJson([entries: names]).getBytes('UTF-8')
    }

    private static void file(HttpExchange exchange, MemberFiles files, String rel, String type, String cache) {
        final Long size = files.size(rel)
        if( size == null ) {
            text(exchange, 404, 'not found')
            return
        }
        final Headers headers = exchange.responseHeaders
        headers.set('Content-Type', type)
        headers.set('Accept-Ranges', 'bytes')
        headers.set('Cache-Control', cache)
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
        exchange.sendResponseHeaders(status, length)
        final InputStream input = files.open(rel, start, length)
        try {
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
