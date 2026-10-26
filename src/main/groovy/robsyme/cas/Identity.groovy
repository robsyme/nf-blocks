package robsyme.cas

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import groovy.transform.CompileStatic
import groovy.transform.Canonical
import groovy.util.logging.Slf4j

/** Who a run acts as, and which source said so. Only the name is ever written into a block. */
@Canonical
@CompileStatic
class Principal {
    static final String ANONYMOUS = CasConfig.DEFAULT_ASSERTED_BY

    String name

    /** One of 'config', 'platform', 'env', 'git' or 'anonymous'. */
    String source

    /** True for the anonymous source, and for a blank name or one that reads `anonymous` in any case. */
    boolean isAnonymous() { source == 'anonymous' || isAnonymousName(name) }

    /** A blank name, or `anonymous` after trimming and ignoring case. */
    static boolean isAnonymousName(Object value) {
        final String s = value == null ? '' : value.toString().trim()
        return s.isEmpty() || s.equalsIgnoreCase(ANONYMOUS)
    }
}

/** Reads `git config --get user.email`. */
interface GitEmail {
    String userEmail() throws IOException
}

/**
 * Runs git with a 2 second limit. A non-zero exit gives null. On macOS the
 * {@code /usr/bin/git} shim opens the "install the Command Line Tools" dialog
 * when they are missing, so when that is the git that would run, it first asks
 * {@code xcode-select -p} (2 seconds too) and skips git when that fails. Every
 * stream of each process is closed. The fields are test seams.
 */
@Slf4j
@CompileStatic
class ProcessGitEmail implements GitEmail {

    static final long TIMEOUT_SECONDS = 2
    /** Apple's git stub: without the Command Line Tools, running it pops an install dialog, so it is checked first. */
    static final String MACOS_SHIM = '/usr/bin/git'

    String osName = System.getProperty('os.name')

    /** The absolute path of the git that would run, or null when none is on PATH. */
    Closure<String> whichGit = { -> onPath('git', System.getenv('PATH')) } as Closure<String>

    /** The exit status of {@code xcode-select -p}, or -1 when it could not run or took too long. */
    Closure<Integer> xcodeSelect = { -> runXcodeSelect() } as Closure<Integer>

    /** Starts a command; stderr is discarded. */
    Closure<Process> starter = { List<String> command ->
        new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.DISCARD).start()
    } as Closure<Process>

    @Override
    String userEmail() throws IOException {
        if( !mayRunGit() )
            return null
        final Process p = starter.call(['git', 'config', '--get', 'user.email'])
        try {
            p.outputStream.close()
            if( !p.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS) ) {
                p.destroyForcibly()
                return null
            }
            if( p.exitValue() != 0 )
                return null
            return new String(p.inputStream.readAllBytes(), 'UTF-8')
        }
        catch( InterruptedException e ) {
            Thread.currentThread().interrupt()
            p.destroyForcibly()
            throw new IOException('interrupted')
        }
        finally {
            closeStreams(p)
        }
    }

    /**
     * False only on macOS when git is the {@code /usr/bin/git} shim and the
     * Command Line Tools are missing. A link whose real path is the shim is
     * the shim. No git found on {@code PATH} counts as the shim: with
     * {@code PATH} unset, the process lookup falls back to {@code /usr/bin}.
     */
    private boolean mayRunGit() {
        if( !(osName ?: '').startsWith('Mac') )
            return true
        final String git = whichGit.call()
        if( git != null && git != MACOS_SHIM )
            return true
        final Integer status = xcodeSelect.call()
        if( status == 0 )
            return true
        log.debug("nf-blocks: not running ${MACOS_SHIM}: xcode-select -p exited ${status}, so the Command Line Tools are missing and git would ask to install them")
        return false
    }

    /**
     * The first executable {@code name} in {@code path}'s directories, with
     * symlinks resolved, so a link to {@code /usr/bin/git} reads as the shim.
     * A link whose real path cannot be read is returned normalised, as found.
     * Null when there is none.
     */
    static String onPath(String name, String path) {
        if( !path )
            return null
        for( String dir : path.split(File.pathSeparator) ) {
            if( !dir )
                continue
            Path candidate = null
            try {
                candidate = Paths.get(dir).resolve(name).normalize()
            }
            catch( Exception ignored ) {
                // not a path on this system: look in the next directory
            }
            if( candidate != null && Files.isRegularFile(candidate) && Files.isExecutable(candidate) )
                return realPath(candidate)
        }
        return null
    }

    private static String realPath(Path found) {
        try {
            return found.toRealPath().toString()
        }
        catch( IOException | SecurityException ignored ) {
            return found.toString()
        }
    }

    private static int runXcodeSelect() {
        final Process p
        try {
            p = new ProcessBuilder('/usr/bin/xcode-select', '-p')
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .redirectError(ProcessBuilder.Redirect.DISCARD)
                .start()
        }
        catch( IOException e ) {
            return -1
        }
        try {
            if( !p.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS) ) {
                p.destroyForcibly()
                return -1
            }
            return p.exitValue()
        }
        catch( InterruptedException e ) {
            Thread.currentThread().interrupt()
            p.destroyForcibly()
            return -1
        }
        finally {
            closeStreams(p)
        }
    }

    private static void closeStreams(Process p) {
        final List<Closeable> streams = [p.outputStream, p.inputStream, p.errorStream] as List<Closeable>
        for( Closeable c : streams ) {
            try {
                c?.close()
            }
            catch( IOException ignored ) {
                // nothing more to release
            }
        }
    }
}

/**
 * Resolves `asserted_by`: an explicit `cas.asserted_by`, then the Seqera
 * Platform owner of the configured token, then NXF_BLOCKS_USER, then
 * `git config user.email`, then 'anonymous'. See docs/identity.md.
 */
@CompileStatic
class Identity {

    static final String DEFAULT_ENDPOINT = 'https://api.cloud.seqera.io'

    /** What a run's warnings say will read 'anonymous'. */
    static final String RUN_WHAT = "this run's blocks and claims"

    /** What a command's warnings say will read 'anonymous'. */
    static final String COMMAND_WHAT = 'what this command writes'

    private static final String HOW_TO_SET = 'Set NXF_BLOCKS_USER or git config user.email (see docs/identity.md)'

    /** As the 7-argument form, with a run's wording. */
    static Principal resolve(String configured, Map sessionConfig, Map<String,String> env, GitEmail git,
                             PlatformIdentity platform, Closure<Void> warn) {
        return resolve(configured, sessionConfig, env, git, platform, RUN_WHAT, warn)
    }

    /** {@code what} names what will say 'anonymous' in a warning: {@link #RUN_WHAT} or {@link #COMMAND_WHAT}. */
    static Principal resolve(String configured, Map sessionConfig, Map<String,String> env, GitEmail git,
                             PlatformIdentity platform, String what, Closure<Void> warn) {
        final String explicit = named(configured)
        if( explicit != null )
            return new Principal(explicit, 'config')

        final Object towerScope = sessionConfig?.get('tower')
        final Map tower = towerScope instanceof Map ? (Map) towerScope : [:]
        final String token = clean(tower.get('accessToken')) ?: clean(env.get('TOWER_ACCESS_TOKEN'))
        if( token != null ) {
            final String raw = clean(tower.get('endpoint')) ?: clean(env.get('TOWER_API_ENDPOINT')) ?: DEFAULT_ENDPOINT
            final String endpoint = raw.endsWith('/') ? raw.substring(0, raw.length() - 1) : raw
            String reason
            try {
                final String email = clean(platform.email(endpoint, token))
                if( email != null )
                    return new Principal(email, 'platform')
                reason = 'the answer carries no email'
            }
            catch( Exception e ) {
                reason = (e.message ?: e.class.simpleName).replace(token, '***')
            }
            final Principal next = fallback(env, git)
            if( next.isAnonymous() )
                warn.call("nf-blocks: could not read your Seqera Platform identity (${reason}) and no other identity is configured, so ${what} will say 'anonymous'. ${HOW_TO_SET}".toString())
            else
                warn.call("nf-blocks: could not read your Seqera Platform identity (${reason}); using ${sourceText(next)} instead".toString())
            return next
        }
        final Principal next = fallback(env, git)
        if( next.isAnonymous() )
            warn.call(anonymousWarning(what))
        return next
    }

    /** The warning for a principal that resolved to anonymous with nothing failing on the way. */
    static String anonymousWarning(String what) {
        return "nf-blocks: no identity is configured, so ${what} will say 'anonymous'. ${HOW_TO_SET}".toString()
    }

    /** NXF_BLOCKS_USER, git, then anonymous. Never warns. */
    private static Principal fallback(Map<String,String> env, GitEmail git) {
        final String user = named(env.get('NXF_BLOCKS_USER'))
        if( user != null )
            return new Principal(user, 'env')
        String email = null
        try {
            email = clean(git.userEmail())
        }
        catch( Exception ignored ) {
            // git is missing or broken: the next source decides
        }
        if( email != null )
            return new Principal(email, 'git')
        return new Principal(Principal.ANONYMOUS, 'anonymous')
    }

    /** The words used for a source in warnings and in the "signing as" line. */
    static String sourceText(Principal p) {
        switch( p.source ) {
            case 'config': return 'cas.asserted_by'
            case 'platform': return 'Seqera Platform'
            case 'env': return 'NXF_BLOCKS_USER'
            case 'git': return 'git config user.email'
            default: return 'anonymous'
        }
    }

    /** A name someone typed: blank or `anonymous` (any case) counts as unset, so typing it never opts in. */
    private static String named(Object value) {
        return Principal.isAnonymousName(value) ? null : clean(value)
    }

    private static String clean(Object value) {
        final String s = value == null ? null : value.toString().trim()
        return s ? s : null
    }
}
