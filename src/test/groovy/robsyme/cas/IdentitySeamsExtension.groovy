package robsyme.cas

import java.util.concurrent.atomic.AtomicInteger

import org.spockframework.runtime.extension.IGlobalExtension
import org.spockframework.runtime.extension.IMethodInterceptor
import org.spockframework.runtime.extension.IMethodInvocation
import org.spockframework.runtime.model.FeatureInfo
import org.spockframework.runtime.model.SpecInfo

/**
 * Keeps every spec away from a real Seqera Platform and from the developer's
 * git email, in Gradle and in an IDE runner alike. Before each spec, and again
 * before each iteration, {@link CasSession}'s identity seams are set to a
 * Platform double that refuses every call, a git double that answers null and
 * the process environment without TOWER_* or NXF_BLOCKS_USER; the identity
 * lines already printed are forgotten. What was there before is put back
 * afterwards, so a spec that sets its own seams in setup keeps them for its
 * feature and loses them after.
 */
class IdentitySeamsExtension implements IGlobalExtension {

    /** Calls the refusing Platform double has seen. A spec that expects none can assert on it. */
    static final AtomicInteger platformCalls = new AtomicInteger()

    static final PlatformIdentity REFUSING_PLATFORM = { String endpoint, String token ->
        platformCalls.incrementAndGet()
        throw new IOException('tests never call Platform')
    } as PlatformIdentity

    static final GitEmail NO_GIT = { -> null } as GitEmail

    /** {@code source} without TOWER_* or NXF_BLOCKS_USER. */
    static Map<String, String> cleanEnv(Map<String, String> source) {
        return source.findAll { String k, String v -> !k.startsWith('TOWER_') && k != 'NXF_BLOCKS_USER' }
            .asImmutable() as Map<String, String>
    }

    /** The seams as they were, for {@link #restore}. */
    static final class Saved {
        PlatformIdentity platform
        GitEmail git
        Map<String, String> env
    }

    /** Installs the test seams over {@code processEnv} and returns what they replaced. */
    static Saved install(Map<String, String> processEnv) {
        final Saved saved = new Saved(platform: CasSession.platformIdentity, git: CasSession.gitEmail, env: CasSession.env)
        CasSession.platformIdentity = REFUSING_PLATFORM
        CasSession.gitEmail = NO_GIT
        CasSession.env = cleanEnv(processEnv)
        CasSession.forgetNotices()
        return saved
    }

    static void restore(Saved saved) {
        CasSession.platformIdentity = saved.platform
        CasSession.gitEmail = saved.git
        CasSession.env = saved.env
    }

    private static final IMethodInterceptor AROUND = { IMethodInvocation invocation ->
        final Saved saved = install(System.getenv())
        try {
            invocation.proceed()
        }
        finally {
            restore(saved)
        }
    } as IMethodInterceptor

    @Override
    void visitSpec(SpecInfo spec) {
        // The spec interceptor covers setupSpec and @Shared initialisers; the
        // iteration interceptor wraps setup, the feature and cleanup.
        spec.addInterceptor(AROUND)
        for( FeatureInfo feature : spec.allFeatures )
            feature.addIterationInterceptor(AROUND)
    }
}
