package robsyme.cas

import java.lang.reflect.Method
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import groovy.json.JsonSlurper
import groovy.transform.CompileStatic

/** Asks Seqera Platform who owns an access token. */
interface PlatformIdentity {

    /** The email of the token's owner, or null if the answer carries none. Throws on any failure. */
    String email(String endpoint, String token) throws IOException
}

/**
 * {@code GET <endpoint>/user-info} with a bearer token. The connect is bounded by
 * {@code timeoutMillis}, and so is the whole answer, body included: the request's
 * own timeout stops counting once the headers arrive, so the call waits on the
 * future with a deadline. A slow or down endpoint costs a run a few seconds at
 * most. The token is never put into an exception. Each call builds its own
 * client and shuts it down before returning.
 */
@CompileStatic
class HttpPlatformIdentity implements PlatformIdentity {

    static final int DEFAULT_TIMEOUT_MILLIS = 5000

    private final int timeoutMillis

    HttpPlatformIdentity() {
        this(DEFAULT_TIMEOUT_MILLIS)
    }

    HttpPlatformIdentity(int timeoutMillis) {
        this.timeoutMillis = timeoutMillis
    }

    /** The client for one call; a test seam. */
    protected HttpClient newClient() {
        return HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(timeoutMillis))
            .build()
    }

    /**
     * Stops the client and any exchange still in flight. HttpClient has
     * shutdownNow from Java 21; on Java 17 the method is absent, and the
     * client's threads end once nothing refers to it.
     */
    protected void release(HttpClient client) {
        if( SHUTDOWN_NOW == null )
            return
        try {
            SHUTDOWN_NOW.invoke(client)
        }
        catch( Exception ignored ) {
            // nothing more to release
        }
    }

    private static final Method SHUTDOWN_NOW = shutdownNowMethod()

    private static Method shutdownNowMethod() {
        try {
            return HttpClient.getMethod('shutdownNow')
        }
        catch( NoSuchMethodException e ) {
            return null
        }
    }

    @Override
    String email(String endpoint, String token) throws IOException {
        final HttpClient client = newClient()
        try {
            return ask(client, endpoint, token)
        }
        finally {
            release(client)
        }
    }

    private String ask(HttpClient client, String endpoint, String token) throws IOException {
        final String base = endpoint.endsWith('/') ? endpoint.substring(0, endpoint.length() - 1) : endpoint
        final HttpRequest request
        try {
            request = HttpRequest.newBuilder(URI.create(base + '/user-info'))
                .timeout(Duration.ofMillis(timeoutMillis))
                .header('Authorization', 'Bearer ' + token)
                .header('Accept', 'application/json')
                .GET()
                .build()
        }
        catch( IllegalArgumentException e ) {
            throw new IOException('the endpoint is not a valid URL')
        }
        final CompletableFuture<HttpResponse<String>> pending = client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        final HttpResponse<String> response
        try {
            response = pending.get(timeoutMillis, TimeUnit.MILLISECONDS)
        }
        catch( TimeoutException e ) {
            pending.cancel(true)
            throw new IOException("no complete answer within ${timeoutMillis} ms".toString())
        }
        catch( InterruptedException e ) {
            pending.cancel(true)
            Thread.currentThread().interrupt()
            throw new IOException('interrupted')
        }
        catch( ExecutionException e ) {
            final Throwable cause = e.cause ?: e
            throw new IOException(cause.message ?: cause.class.simpleName)
        }
        if( response.statusCode() < 200 || response.statusCode() >= 300 )
            throw new IOException("HTTP ${response.statusCode()}".toString())
        final Object parsed
        try {
            parsed = new JsonSlurper().parseText(response.body())
        }
        catch( Exception e ) {
            throw new IOException('the answer is not JSON')
        }
        final Object user = parsed instanceof Map ? ((Map) parsed).get('user') : null
        final Object email = user instanceof Map ? ((Map) user).get('email') : null
        return email instanceof String ? (String) email : null
    }
}
