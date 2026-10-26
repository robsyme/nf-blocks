package robsyme.cas

import spock.lang.Specification

class CopyErrorsTest extends Specification {

    static final Map<String, String> ROOTS = ['lab': '/data/cas/lab', 'org': 's3://org-bucket/cas']

    def 'a message is the exception class simple name, a colon and the message'() {
        expect:
        CopyErrors.describe(new IOException('the bucket went away'), [:]) == 'IOException: the bucket went away'
        CopyErrors.describe(new IllegalStateException(), [:]) == 'IllegalStateException'
    }

    def 'a member root becomes its alias, local or S3, longest root first'() {
        expect:
        CopyErrors.redact('cannot write /data/cas/lab/blocks/ab/cd', ROOTS) == 'cannot write lab:/blocks/ab/cd'
        CopyErrors.redact('PUT s3://org-bucket/cas/blocks/ab failed', ROOTS) == 'PUT org:/blocks/ab failed'
        CopyErrors.redact('at /data/cas/lab', ['a': '/data', 'b': '/data/cas/lab']) == 'at b:'
    }

    def 'any other absolute local path is [local path], whatever it names'() {
        expect:
        CopyErrors.redact('no such file /home/rob/.cache/idx/index.sqlite.', ROOTS) == 'no such file [local path].'
        CopyErrors.redact("spool '/var/tmp/t/spool-1' is full", ROOTS) == "spool '[local path]' is full"
        CopyErrors.redact("open ${System.getProperty('user.home')}/x failed", ROOTS) == 'open [local path]'
        CopyErrors.redact('C:\\Users\\rob\\cas failed', ROOTS) == '[local path] failed'
    }

    def 'text that is not a local path stays'() {
        expect:
        CopyErrors.redact('1/3 of and/or https://example.org/a/b s3://other/key lab:/blocks/x', ROOTS) ==
            '1/3 of and/or https://example.org/a/b s3://other/key lab:/blocks/x'
    }

    def 'a file: URI with a local path is redacted, or an alias when it names a member root'() {
        expect:
        CopyErrors.redact('cannot open file:///home/rob/x now', ROOTS) == 'cannot open [local path] now'
        CopyErrors.redact('cannot open file:/home/rob/x now', ROOTS) == 'cannot open [local path] now'
        CopyErrors.redact('cannot open file:///data/cas/lab/blocks/ab', ROOTS) == 'cannot open lab:/blocks/ab'
    }

    def 'a path with spaces under the home, tmp or working directory leaks nothing after its first space'() {
        given:
        final String home = System.getProperty('user.home')
        final String tmp = System.getProperty('java.io.tmpdir').replaceAll('/+$', '')
        final String cwd = System.getProperty('user.dir')

        expect:
        CopyErrors.redact("open ${home}/Library/Application Support/nf-blocks/index.sqlite failed", ROOTS) == 'open [local path]'
        CopyErrors.redact("spool ${tmp}/my spool/x.part is full", ROOTS) == 'spool [local path]'
        CopyErrors.redact("in ${cwd}/some project/y", ROOTS) == 'in [local path]'
        CopyErrors.redact("open '${home}/Application Support/z'", ROOTS) == "open '[local path]'"
    }

    def 'the home, tmp and working-directory prefixes match whole paths only, never inside a URL or after an alias'() {
        given:
        final Map<String, String> saved = ['user.home', 'java.io.tmpdir', 'user.dir'].collectEntries { [(it): System.getProperty(it)] }
        System.setProperty('java.io.tmpdir', '/tmp')
        System.setProperty('user.home', '/home/rob')
        System.setProperty('user.dir', '/work/here')

        expect:
        CopyErrors.redact('GET s3://other/tmp/key', ROOTS) == 'GET s3://other/tmp/key'
        CopyErrors.redact('GET https://h/home/rob/x', ROOTS) == 'GET https://h/home/rob/x'
        CopyErrors.redact('at lab:/tmp/x', ROOTS) == 'at lab:/tmp/x'
        CopyErrors.redact('at cas://bafy123/tmp', ROOTS) == 'at cas://bafy123/tmp'
        CopyErrors.redact('at /tmp2/x and /tmp/x', ROOTS) == 'at [local path] and [local path]'

        cleanup:
        saved.each { k, v -> System.setProperty(k, v) }
    }

    def 'a path under home, tmp or the working directory is redacted through to a hard terminator, spaces and all'() {
        given:
        final String home = System.getProperty('user.home')

        expect:
        CopyErrors.redact("open ${home}/OneDrive - Seqera/x ok", ROOTS) == 'open [local path]'
        CopyErrors.redact("open ${home}/Desktop/Screen Shot 2024.png failed", ROOTS) == 'open [local path]'
        CopyErrors.redact("open ${home}/My Mac/x ok", ROOTS) == 'open [local path]'
        CopyErrors.redact("open ${home}/Library/Mobile Documents Sync/x ok", ROOTS) == 'open [local path]'
        CopyErrors.redact("open ${home}/Documents/My Report.pdf failed", ROOTS) == 'open [local path]'
        CopyErrors.redact("open ${home}/Library/Application Support failed", ROOTS) == 'open [local path]'
        CopyErrors.redact("read ${home}/x and/or write", ROOTS) == 'read [local path]'
        CopyErrors.redact("open '${home}/a b/c' failed", ROOTS) == "open '[local path]' failed"
        CopyErrors.redact("open (${home}/a b/c), then", ROOTS) == 'open ([local path]), then'
        CopyErrors.redact("${home}/My Mac/x: Not a directory", ROOTS) == '[local path]: Not a directory'
    }

    def 'a following scheme:// URL is kept after a home path'() {
        given:
        final String home = System.getProperty('user.home')

        expect:
        CopyErrors.redact("copy ${home}/My Mac/x s3://b/k", ROOTS) == 'copy [local path] s3://b/k'
    }

    def 'file: is a scheme only at a word start'() {
        expect: 'profile: keeps its "file", and the path after it goes as one after any word: does'
        CopyErrors.redact('see profile:/blocks/x', ROOTS) == 'see profile:[local path]'
        CopyErrors.redact('see file:///home/rob/x', ROOTS) == 'see [local path]'
    }

    def 'a path right after word: is redacted, unless the word is a member alias or the colon starts ://'() {
        expect:
        CopyErrors.redact('cannot open jar:file:/opt/app/lib/x.jar!/META-INF', ROOTS) == 'cannot open jar:[local path]'
        CopyErrors.redact('url jdbc:sqlite:/var/db/index.sqlite failed', ROOTS) == 'url jdbc:sqlite:[local path] failed'
        CopyErrors.redact('bad location:/srv/cas/x', ROOTS) == 'bad location:[local path]'
        and: 'an alias the member-root replacement wrote, or one already in the text, stays'
        CopyErrors.redact('cannot write /data/cas/lab/blocks/ab', ROOTS) == 'cannot write lab:/blocks/ab'
        CopyErrors.redact('err:/data/x', ['d': '/data']) == 'err:d:/x'
        CopyErrors.redact('at lab:/blocks/x', ROOTS) == 'at lab:/blocks/x'
        and: 'a URL stays'
        CopyErrors.redact('GET s3://other/key and https://h/a/b', ROOTS) == 'GET s3://other/key and https://h/a/b'
    }

    def 'a path under home, tmp or the working directory right after word: is redacted, unless the word is an alias'() {
        given:
        final Map<String, String> saved = ['user.home', 'java.io.tmpdir', 'user.dir'].collectEntries { [(it): System.getProperty(it)] }
        System.setProperty('java.io.tmpdir', '/tmp')
        System.setProperty('user.home', '/home/rob')
        System.setProperty('user.dir', '/work/here')

        expect:
        CopyErrors.redact('cannot open jdbc:sqlite:/home/rob/My Stores/index.sqlite', ROOTS) == 'cannot open jdbc:sqlite:[local path]'
        CopyErrors.redact('at lab:/tmp/x', ROOTS) == 'at lab:/tmp/x'

        cleanup:
        saved.each { k, v -> System.setProperty(k, v) }
    }

    def 'a drive path with forward slashes is a local path; a one-letter alias is not a drive'() {
        expect:
        CopyErrors.redact('open C:/Users/rob/cas failed', ROOTS) == 'open [local path] failed'
        CopyErrors.redact('open c:/x', ROOTS) == 'open [local path]'
        CopyErrors.redact('at /data/x', ['d': '/data']) == 'at d:/x'
    }

    def 'a root after word: is aliased, but not inside scheme://'() {
        expect:
        CopyErrors.redact('err:/data/x', ['d': '/data']) == 'err:d:/x'
        CopyErrors.redact('GET https://h/data/x', ['d': '/data']) == 'GET https://h/data/x'
    }

    def 'a root rewrites only a whole path: not a longer sibling, not inside a URL'() {
        given:
        final Map<String, String> roots = ['d': '/data']

        expect:
        CopyErrors.redact('at /data/x and /data. then /data', roots) == 'at d:/x and d:. then d:'
        CopyErrors.redact('at /data2/x', roots) == 'at [local path]'
        CopyErrors.redact('GET https://h/data/x', roots) == 'GET https://h/data/x'
    }

    def 'an exception class with no simple name is named by its class name'() {
        expect:
        CopyErrors.describe(new IOException('x') { }, [:]).startsWith('robsyme.cas.CopyErrorsTest$')
    }

    def 'rootsOf reads each configured location, with the absolute form of a relative one'() {
        when:
        final Map<String, String> roots = CopyErrors.rootsOf([cas: [stores: [lab: [location: '/data/lab'], rel: [location: 'relstore'],
                                                                        org: [location: 's3://b/p']]]])

        then:
        roots.lab == '/data/lab'
        roots.org == 's3://b/p'
        roots.rel == new File('relstore').absolutePath
    }

    def 'rootsOf normalizes a location, so one with . or .. matches the path it names'() {
        when:
        final Map<String, String> roots = CopyErrors.rootsOf([cas: [stores: [lab: [location: '/data/./cas/../lab/'], rel: [location: './a/../relstore']]]])

        then:
        roots.lab == '/data/lab'
        roots.rel == new File('relstore').absolutePath
        CopyErrors.redact('cannot write /data/lab/blocks/ab', roots) == 'cannot write lab:/blocks/ab'
    }

    def 'a credential in the exception message is redacted, as the RunManifest scrub redacts it'() {
        when:
        final String text = CopyErrors.describe(new IOException('PUT failed for AKIAIOSFODNN7EXAMPLE at /data/cas/lab/x with Authorization: Bearer abcdef0123456789abcdef0123'), ROOTS)

        then:
        !text.contains('AKIAIOSFODNN7EXAMPLE')
        !text.contains('abcdef0123456789abcdef0123')
        text.contains('[secret]')
        text.contains('lab:/x')
        text.startsWith('IOException: PUT failed for ')
    }
}
