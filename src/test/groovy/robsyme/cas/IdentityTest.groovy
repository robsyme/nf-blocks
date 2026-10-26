package robsyme.cas

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import spock.lang.Specification

/**
 * How `asserted_by` resolves to a principal: explicit config, Seqera Platform,
 * NXF_BLOCKS_USER, git user.email, then anonymous.
 */
class IdentityTest extends Specification {

    static final String TOKEN = 'secret-token-value'

    List<String> warnings = []
    Closure<Void> warn = { String m -> warnings << m; null } as Closure<Void>
    List<List<String>> platformCalls = []

    private PlatformIdentity platform(Object answer) {
        return { String endpoint, String token ->
            platformCalls << [endpoint, token]
            if( answer instanceof Throwable )
                throw (Throwable) answer
            return (String) answer
        } as PlatformIdentity
    }

    private static GitEmail git(Object answer) {
        return { ->
            if( answer instanceof Throwable )
                throw (Throwable) answer
            return (String) answer
        } as GitEmail
    }

    def 'an explicit value wins over every other source and Platform is never called'() {
        when:
        def p = Identity.resolve('gate', [tower: [accessToken: 't']], [NXF_BLOCKS_USER: 'ana'], git('g@x'), platform('p@x'), warn)

        then:
        p.name == 'gate'
        p.source == 'config'
        platformCalls.isEmpty()
        warnings.isEmpty()
    }

    def 'a token from the environment asks Platform at the default endpoint'() {
        when:
        def p = Identity.resolve(null, [:], [TOWER_ACCESS_TOKEN: TOKEN], git('g@x'), platform('ada@example.org'), warn)

        then:
        p.name == 'ada@example.org'
        p.source == 'platform'
        platformCalls == [['https://api.cloud.seqera.io', TOKEN]]
    }

    def 'the session tower scope sets the endpoint and token, and a trailing slash is stripped'() {
        when:
        def p = Identity.resolve(null, [tower: [accessToken: 't', endpoint: 'https://tower.example/api/']], [:], git(null), platform('a@b'), warn)

        then:
        p.source == 'platform'
        platformCalls == [['https://tower.example/api', 't']]
    }

    def 'TOWER_API_ENDPOINT is used when the config has no endpoint'() {
        when:
        Identity.resolve(null, [tower: [accessToken: 't']], [TOWER_API_ENDPOINT: 'https://e.example/'], git(null), platform('a@b'), warn)

        then:
        platformCalls == [['https://e.example', 't']]
    }

    def 'a Platform 401 falls through to NXF_BLOCKS_USER with one warning that hides the token'() {
        when:
        def p = Identity.resolve(null, [:], [TOWER_ACCESS_TOKEN: TOKEN, NXF_BLOCKS_USER: 'ana'], git('g@x'), platform(new IOException('HTTP 401')), warn)

        then:
        p.name == 'ana'
        p.source == 'env'
        warnings.size() == 1
        warnings[0].contains('HTTP 401')
        warnings[0].contains('NXF_BLOCKS_USER')
        !warnings[0].contains(TOKEN)
    }

    def 'a Platform timeout falls through with one warning'() {
        when:
        def p = Identity.resolve(null, [:], [TOWER_ACCESS_TOKEN: TOKEN], git('g@x'), platform(new SocketTimeoutException('Read timed out')), warn)

        then:
        p.name == 'g@x'
        p.source == 'git'
        warnings.size() == 1
        warnings[0].contains('git')
        !warnings[0].contains(TOKEN)
    }

    def 'a blank NXF_BLOCKS_USER counts as unset and git is trimmed but not lowercased'() {
        when:
        def p = Identity.resolve('  ', [:], [NXF_BLOCKS_USER: '  '], git(' Someone@Example.org\n'), platform('x'), warn)

        then:
        p.name == 'Someone@Example.org'
        p.source == 'git'
        platformCalls.isEmpty()
        warnings.isEmpty()
    }

    def 'a blank token counts as unset'() {
        when:
        def p = Identity.resolve(null, [tower: [accessToken: ' ']], [TOWER_ACCESS_TOKEN: ''], git('g@x'), platform('x'), warn)

        then:
        p.source == 'git'
        platformCalls.isEmpty()
    }

    def 'with nothing configured the principal is anonymous and warns once'() {
        when:
        def p = Identity.resolve(null, [:], [:], git(gitAnswer), platform('x'), warn)

        then:
        p.name == 'anonymous'
        p.source == 'anonymous'
        p.isAnonymous()
        warnings.size() == 1
        warnings[0].contains("'anonymous'")
        warnings[0].contains('NXF_BLOCKS_USER')
        warnings[0].contains('docs/identity.md')

        where:
        gitAnswer << [new IOException('git: not found'), '']
    }

    def 'a Platform failure with nothing else prints one warning that says what failed (#what)'() {
        when:
        def p = Identity.resolve(null, [:], [TOWER_ACCESS_TOKEN: TOKEN], git(''), platform(new IOException("HTTP 401 for ${TOKEN}".toString())), what, warn)

        then:
        p.source == 'anonymous'
        warnings == ["nf-blocks: could not read your Seqera Platform identity (HTTP 401 for ***) and no other identity is configured, " +
            "so ${what} will say 'anonymous'. Set NXF_BLOCKS_USER or git config user.email (see docs/identity.md)".toString()]

        where:
        what << [Identity.RUN_WHAT, Identity.COMMAND_WHAT]
    }

    def 'the two wordings'() {
        expect:
        Identity.RUN_WHAT == "this run's blocks and claims"
        Identity.COMMAND_WHAT == 'what this command writes'
    }

    def 'a run with nothing configured keeps the run wording; a command says what it writes'() {
        when:
        Identity.resolve(null, [:], [:], git(''), platform('x'), warn)
        Identity.resolve(null, [:], [:], git(''), platform('x'), Identity.COMMAND_WHAT, warn)

        then:
        warnings == [
            "nf-blocks: no identity is configured, so this run's blocks and claims will say 'anonymous'. Set NXF_BLOCKS_USER or git config user.email (see docs/identity.md)",
            "nf-blocks: no identity is configured, so what this command writes will say 'anonymous'. Set NXF_BLOCKS_USER or git config user.email (see docs/identity.md)",
        ]
    }

    def 'a Platform failure that falls through to git keeps the single using-instead warning'() {
        when:
        Identity.resolve(null, [:], [TOWER_ACCESS_TOKEN: TOKEN], git('g@x'), platform(new IOException('HTTP 401')), Identity.COMMAND_WHAT, warn)

        then:
        warnings == ['nf-blocks: could not read your Seqera Platform identity (HTTP 401); using git config user.email instead']
    }

    // ------------------------------------------------------------ git

    /** A process double: what git would print, its exit, and which streams were closed. */
    static class FakeProcess extends Process {
        final List<String> closed = []
        final int exit
        final InputStream stdoutStream
        final InputStream stderrStream
        final OutputStream stdinStream

        FakeProcess(String stdout, int exit) {
            this.exit = exit
            final List<String> seen = closed
            stdoutStream = new ByteArrayInputStream(stdout.getBytes('UTF-8')) { @Override void close() { seen << 'stdout'; super.close() } }
            stderrStream = new ByteArrayInputStream(new byte[0]) { @Override void close() { seen << 'stderr'; super.close() } }
            stdinStream = new ByteArrayOutputStream() { @Override void close() { seen << 'stdin'; super.close() } }
        }

        @Override OutputStream getOutputStream() { stdinStream }
        @Override InputStream getInputStream() { stdoutStream }
        @Override InputStream getErrorStream() { stderrStream }
        @Override int waitFor() { exit }
        @Override boolean waitFor(long timeout, java.util.concurrent.TimeUnit unit) { true }
        @Override int exitValue() { exit }
        @Override void destroy() { }
    }

    List<List<String>> started = []
    int xcodeSelectCalls = 0

    private ProcessGitEmail gitRunner(String os, String gitPath, int xcodeExit, FakeProcess process = new FakeProcess('rob@x\n', 0)) {
        final ProcessGitEmail g = new ProcessGitEmail()
        g.osName = os
        g.whichGit = { -> gitPath } as Closure<String>
        g.xcodeSelect = { -> xcodeSelectCalls++; xcodeExit } as Closure<Integer>
        g.starter = { List<String> command -> started << command; process } as Closure<Process>
        return g
    }

    def 'on macOS without the Command Line Tools, /usr/bin/git is never run'() {
        when:
        def email = gitRunner('Mac OS X', '/usr/bin/git', 2).userEmail()

        then:
        email == null
        xcodeSelectCalls == 1
        started.isEmpty()
    }

    def 'on macOS with the Command Line Tools, /usr/bin/git is run'() {
        when:
        def email = gitRunner('Mac OS X', '/usr/bin/git', 0).userEmail()

        then:
        email.trim() == 'rob@x'
        xcodeSelectCalls == 1
        started == [['git', 'config', '--get', 'user.email']]
    }

    def 'on macOS with no git found on PATH (PATH unset), git counts as the shim: xcode-select exits #xcodeExit'() {
        when:
        def email = gitRunner('Mac OS X', null, xcodeExit).userEmail()

        then:
        xcodeSelectCalls == 1
        started.size() == runs
        (email?.trim()) == answer

        where:
        xcodeExit | runs | answer
        2         | 0    | null
        0         | 1    | 'rob@x'
    }

    def 'on macOS a git other than /usr/bin/git runs without asking xcode-select'() {
        when:
        def email = gitRunner('Mac OS X', '/opt/homebrew/bin/git', 2).userEmail()

        then:
        email.trim() == 'rob@x'
        xcodeSelectCalls == 0
        started.size() == 1
    }

    def 'on Linux xcode-select is never consulted'() {
        when:
        def email = gitRunner('Linux', '/usr/bin/git', 2).userEmail()

        then:
        email.trim() == 'rob@x'
        xcodeSelectCalls == 0
        started.size() == 1
    }

    def 'the first git on PATH is the one that would run'() {
        given:
        def dir = java.nio.file.Files.createTempDirectory('git-path')
        def bin = java.nio.file.Files.createDirectories(dir.resolve('bin'))
        def git = java.nio.file.Files.createFile(bin.resolve('git'))
        git.toFile().setExecutable(true)

        expect:
        ProcessGitEmail.onPath('git', "${dir.resolve('none')}${File.pathSeparator}${bin}/${File.pathSeparator}/usr/bin".toString()) == git.toRealPath().toString()
        ProcessGitEmail.onPath('git', dir.resolve('none').toString()) == null
        ProcessGitEmail.onPath('git', null) == null

        cleanup:
        dir.toFile().deleteDir()
    }

    def 'a git on PATH that is a symlink is judged by the file it points to'() {
        given: 'bin/git, a link to an executable elsewhere'
        def dir = java.nio.file.Files.createTempDirectory('git-link')
        def real = java.nio.file.Files.createFile(dir.resolve('real-git'))
        real.toFile().setExecutable(true)
        def bin = java.nio.file.Files.createDirectories(dir.resolve('bin'))
        java.nio.file.Files.createSymbolicLink(bin.resolve('git'), real)

        expect:
        ProcessGitEmail.onPath('git', bin.toString()) == real.toRealPath().toString()

        cleanup:
        dir.toFile().deleteDir()
    }

    def 'a git on PATH that is a symlink to nothing is skipped'() {
        given: 'dangling/git points at a file that does not exist; bin/git is real'
        def dir = java.nio.file.Files.createTempDirectory('git-dangling')
        def dangling = java.nio.file.Files.createDirectories(dir.resolve('dangling'))
        java.nio.file.Files.createSymbolicLink(dangling.resolve('git'), dir.resolve('gone'))
        def bin = java.nio.file.Files.createDirectories(dir.resolve('bin'))
        def git = java.nio.file.Files.createFile(bin.resolve('git'))
        git.toFile().setExecutable(true)

        expect:
        ProcessGitEmail.onPath('git', "${dangling}${File.pathSeparator}${bin}".toString()) == git.toRealPath().toString()
        ProcessGitEmail.onPath('git', dangling.toString()) == null

        cleanup:
        dir.toFile().deleteDir()
    }

    @spock.lang.Requires({ java.nio.file.Files.isRegularFile(java.nio.file.Paths.get('/usr/bin/git')) &&
        java.nio.file.Paths.get('/usr/bin/git').toRealPath().toString() == '/usr/bin/git' })
    def 'on macOS a symlink to /usr/bin/git counts as the shim, so xcode-select is asked first'() {
        given: 'the first git on PATH is a link to /usr/bin/git, and the Command Line Tools are missing'
        def dir = java.nio.file.Files.createTempDirectory('git-shim-link')
        java.nio.file.Files.createSymbolicLink(dir.resolve('git'), java.nio.file.Paths.get('/usr/bin/git'))
        final ProcessGitEmail g = gitRunner('Mac OS X', null, 2)
        g.whichGit = { -> ProcessGitEmail.onPath('git', dir.toString()) } as Closure<String>

        when:
        def email = g.userEmail()

        then:
        email == null
        xcodeSelectCalls == 1
        started.isEmpty()

        cleanup:
        dir.toFile().deleteDir()
    }

    def "git's streams are closed whatever it answers (exit #exit)"() {
        given:
        final FakeProcess process = new FakeProcess('rob@x\n', exit)

        when:
        gitRunner('Linux', '/usr/bin/git', 0, process).userEmail()

        then:
        process.closed as Set == ['stdin', 'stdout', 'stderr'] as Set

        where:
        exit << [0, 1]
    }

    // ------------------------------------------------------------ Platform

    /** Records each client the identity built and each one it released. */
    static class RecordingPlatform extends HttpPlatformIdentity {
        final List<java.net.http.HttpClient> built = []
        final List<java.net.http.HttpClient> released = []
        RecordingPlatform(int timeout) { super(timeout) }
        @Override protected java.net.http.HttpClient newClient() { def c = super.newClient(); built << c; c }
        @Override protected void release(java.net.http.HttpClient client) { released << client; super.release(client) }
    }

    def 'HttpPlatformIdentity releases its client after an answer, a refusal and a timeout (#path)'() {
        given:
        def server = serve { ex -> if( ex.requestURI.path.contains('slow') ) Thread.sleep(1500); reply(ex, code, '{"user":{"email":"a@b"}}') }
        def platform = new RecordingPlatform(300)

        when:
        try { platform.email("http://127.0.0.1:${server.address.port}/${path}".toString(), 'tok') } catch( IOException ignored ) { }

        then:
        platform.built.size() == 1
        platform.released == platform.built
        // HttpClient.awaitTermination is Java 21; below that, released == built is the check.
        Runtime.version().feature() < 21 || platform.built[0].awaitTermination(java.time.Duration.ofSeconds(5))

        cleanup:
        server.stop(0)

        where:
        path   | code
        'ok'   | 200
        'no'   | 401
        'slow' | 200
    }

    def 'a configured or NXF_BLOCKS_USER value of anonymous counts as unset and falls through to git'() {
        when:
        def p = Identity.resolve(configured, [:], env, git('g@x'), platform('x'), warn)

        then:
        p.name == 'g@x'
        p.source == 'git'
        !p.isAnonymous()
        warnings.isEmpty()

        where:
        configured    | env
        'anonymous'   | [:]
        ' ANONYMOUS ' | [:]
        null          | [NXF_BLOCKS_USER: ' Anonymous ']
        'Anonymous'   | [NXF_BLOCKS_USER: 'anonymous']
    }

    def 'a principal is anonymous when its name is blank or anonymous, whatever the source'() {
        expect:
        new Principal(name, source).isAnonymous() == anonymous

        where:
        name          | source      | anonymous
        'anonymous'   | 'anonymous' | true
        'anonymous'   | 'config'    | true
        ' Anonymous ' | 'env'       | true
        ''            | 'git'       | true
        null          | 'config'    | true
        'rob@x'       | 'git'       | false
        'gate'        | 'config'    | false
    }

    def 'HttpPlatformIdentity reads user.email with a bearer token'() {
        given:
        String auth = null
        String path = null
        def server = serve { ex ->
            auth = ex.requestHeaders.getFirst('Authorization')
            path = ex.requestURI.path
            reply(ex, 200, '{"user":{"email":"a@b"}}')
        }

        when:
        def email = new HttpPlatformIdentity().email("http://127.0.0.1:${server.address.port}/", 'tok')

        then:
        email == 'a@b'
        auth == 'Bearer tok'
        path == '/user-info'

        cleanup:
        server.stop(0)
    }

    def 'HttpPlatformIdentity throws on a non-2xx answer without the token in the message'() {
        given:
        def server = serve { ex -> reply(ex, 401, '{}') }

        when:
        new HttpPlatformIdentity().email("http://127.0.0.1:${server.address.port}", 'tok-abc')

        then:
        def e = thrown(IOException)
        e.message.contains('401')
        !e.message.contains('tok-abc')

        cleanup:
        server.stop(0)
    }

    def 'HttpPlatformIdentity gives up when the server is slower than its timeout'() {
        given:
        def server = serve { ex -> Thread.sleep(1500); reply(ex, 200, '{"user":{"email":"a@b"}}') }

        when:
        new HttpPlatformIdentity(300).email("http://127.0.0.1:${server.address.port}", 'tok')

        then:
        thrown(IOException)

        cleanup:
        server.stop(0)
    }

    def 'HttpPlatformIdentity gives up when the body stalls after the headers'() {
        given:
        def server = serve { ex ->
            try {
                ex.sendResponseHeaders(200, 100)
                ex.responseBody.write('{"user":{"em'.getBytes('UTF-8'))
                ex.responseBody.flush()
                Thread.sleep(3000)
            }
            catch( Exception ignored ) {
                // the client has gone
            }
            finally {
                ex.close()
            }
        }

        long start = System.nanoTime()

        when:
        new HttpPlatformIdentity(300).email("http://127.0.0.1:${server.address.port}", 'tok')

        then:
        def e = thrown(IOException)
        !e.message.contains('tok')
        (System.nanoTime() - start) / 1_000_000 < 2000

        cleanup:
        server.stop(0)
    }

    private static void reply(HttpExchange ex, int code, String body) {
        try {
            byte[] bytes = body.getBytes('UTF-8')
            ex.sendResponseHeaders(code, bytes.length)
            ex.responseBody.write(bytes)
        }
        catch( IOException ignored ) {
            // the client may have gone already
        }
        finally {
            ex.close()
        }
    }

    private static HttpServer serve(Closure handler) {
        def server = HttpServer.create(new InetSocketAddress(InetAddress.loopbackAddress, 0), 0)
        server.createContext('/', { HttpExchange ex -> handler.call(ex) } as HttpHandler)
        server.start()
        return server
    }
}
