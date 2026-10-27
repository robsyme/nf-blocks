package robsyme.cas.explore

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import groovy.transform.CompileStatic
import robsyme.cas.s3.S3Failures

/**
 * Whether a copy destination can be reached now (members.json): one look at
 * its store-id, every member at once, with a bound on the wait. A member that
 * cannot be reached is greyed out on the page with a reason that says what to
 * do, instead of the copy plan failing on it. An answer is kept for
 * {@link #KEEP_MILLIS}, so a reload after refreshing credentials asks again.
 */
@CompileStatic
class MemberProbe {

    static final long KEEP_MILLIS = 30_000L
    static final long TIMEOUT_MILLIS = 5_000L

    private static final String RELOAD = 'reload this page'

    private final Map<String, MemberFiles> members
    private final long timeoutMillis
    private final Closure<Long> clock
    private final Map<String, Checked> checked = new ConcurrentHashMap<String, Checked>()
    private final ExecutorService pool = Executors.newCachedThreadPool({ Runnable r ->
        final Thread t = new Thread(r, 'nf-blocks-member-probe')
        t.daemon = true
        return t
    } as ThreadFactory)

    MemberProbe(Map<String, MemberFiles> members) {
        this(members, TIMEOUT_MILLIS, { -> System.currentTimeMillis() })
    }

    MemberProbe(Map<String, MemberFiles> members, long timeoutMillis, Closure<Long> clock) {
        this.members = members
        this.timeoutMillis = timeoutMillis
        this.clock = clock
    }

    /** Why each of {@code aliases} cannot be reached now; a member that can is absent. */
    Map<String, String> unreachable(Collection<String> aliases) {
        final long now = clock.call()
        final Map<String, CompletableFuture<String>> asking = new LinkedHashMap<String, CompletableFuture<String>>()
        for( String alias : aliases ) {
            final Checked last = checked.get(alias)
            final MemberFiles files = members.get(alias)
            if( files != null && (last == null || now - last.at >= KEEP_MILLIS) )
                asking.put(alias, CompletableFuture.supplyAsync({ -> reasonFor(files) }, pool))
        }
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
        asking.each { String alias, CompletableFuture<String> answer ->
            checked.put(alias, new Checked(now, await(answer, deadline)))
        }
        final Map<String, String> out = new LinkedHashMap<String, String>()
        for( String alias : aliases ) {
            final String reason = checked.get(alias)?.reason
            if( reason != null )
                out.put(alias, reason)
        }
        return out
    }

    private static String reasonFor(MemberFiles files) {
        try {
            files.open('store-id')?.close()
            return null
        }
        catch( Exception e ) {
            final String credentials = S3Failures.credentials(e)
            if( credentials != null )
                return "${credentials}. Refresh your AWS credentials, then ${RELOAD}".toString()
            return unreached(S3Failures.cause(e))
        }
    }

    private static String unreached(String cause) {
        return "couldn't reach this store (${cause}). ${RELOAD.capitalize()} to try again".toString()
    }

    private static String await(CompletableFuture<String> answer, long deadline) {
        try {
            return answer.get(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)
        }
        catch( TimeoutException e ) {
            return unreached('it did not answer in time')
        }
        catch( ExecutionException e ) {
            return unreached(S3Failures.cause(e.cause ?: e))
        }
        catch( InterruptedException e ) {
            Thread.currentThread().interrupt()
            return unreached('the check was interrupted')
        }
    }

    @CompileStatic
    private static final class Checked {
        final long at
        final String reason
        Checked(long at, String reason) { this.at = at; this.reason = reason }
    }
}
