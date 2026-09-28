package robsyme.cas.trace

import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

import spock.lang.Requires
import spock.lang.Specification
import spock.lang.TempDir

/** Runs the real script with bash in a fake task directory (ticket 06 §3). */
@Requires({ new File('/bin/bash').canExecute() })
class NodeHashScriptTest extends Specification {

    @TempDir Path task

    private static String hex(byte[] b) { MessageDigest.getInstance('SHA-256').digest(b).encodeHex().toString() }

    private void put(String rel, String text) {
        final Path p = task.resolve(rel); Files.createDirectories(p.parent); Files.writeString(p, text)
    }

    private int runScript(Map<String, String> env = [:]) {
        final ProcessBuilder pb = new ProcessBuilder('/bin/bash', '-c', 'set -u\n' + NodeHash.script() + '\necho after=$?')
            .directory(task.toFile()).redirectErrorStream(true)
        pb.environment().remove('NXF_CHDIR')
        pb.environment().putAll(env)
        final Process p = pb.start()
        final String out = p.inputStream.text
        assert out.contains('after=0')
        return p.waitFor()
    }

    private Map<String, String> cas() {
        Files.readAllLines(task.resolve('.command.cas')).collectEntries { String l -> [(l.substring(66)): l.substring(0, 64)] }
    }

    def 'declared outputs are hashed: globs expanded, directories walked, absent optionals and links skipped'() {
        given:
        put('.command.run', """#!/bin/bash
### ---
### name: 'LINKS'
### outputs:
### - 'd'
### - '*.glob.txt'
### - 'maybe.txt'
### - 'name with space.txt'
### ...
""")
        put('d/target.txt', 'target\n')
        put('d/nested/deep.txt', 'deep\n')
        Files.createSymbolicLink(task.resolve('d/rel.txt'), Path.of('target.txt'))
        put('a.glob.txt', 'a\n'); put('b.glob.txt', 'b\n')
        put('name with space.txt', 'x\n')
        put('input.bam', 'staged input')
        Files.createSymbolicLink(task.resolve('c.glob.txt'), task.resolve('input.bam'))
        put('undeclared.txt', 'nope')

        when:
        runScript()

        then:
        cas() == [
            'd/target.txt'       : hex('target\n'.bytes),
            'd/nested/deep.txt'  : hex('deep\n'.bytes),
            'a.glob.txt'         : hex('a\n'.bytes),
            'b.glob.txt'         : hex('b\n'.bytes),
            'name with space.txt': hex('x\n'.bytes),
        ]
        !Files.exists(task.resolve('.command.cas.tmp'))
    }

    def 'it never fails the task: no .command.run, a declared directory that is absent'() {
        when:
        runScript()

        then:
        !Files.exists(task.resolve('.command.cas'))

        when:
        put('.command.run', "### outputs:\n### - 'gone'\n### - 'here.txt'\n### ...\n")
        put('here.txt', 'h\n')
        runScript()

        then: 'the absent directory gets no line; the rest is hashed'
        cas() == ['here.txt': hex('h\n'.bytes)]
    }

    // setReadable(false) does nothing for root, so the file would be hashed and prove nothing.
    @Requires({ System.getProperty('user.name') != 'root' })
    def 'it never fails the task: an unreadable output'() {
        given:
        put('.command.run', "### outputs:\n### - 'locked.txt'\n### ...\n")
        put('locked.txt', 'x')
        task.resolve('locked.txt').toFile().setReadable(false)

        when:
        runScript()

        then:
        notThrown(AssertionError)
    }

    def 'NXF_CHDIR names the task directory when the shell is elsewhere (Fusion)'() {
        given:
        put('.command.run', "### outputs:\n### - 'out.txt'\n### ...\n")
        put('out.txt', 'o\n')
        final Path elsewhere = Files.createTempDirectory('elsewhere')

        when:
        final ProcessBuilder pb = new ProcessBuilder('/bin/bash', '-c', NodeHash.script())
            .directory(elsewhere.toFile())
        pb.environment().put('NXF_CHDIR', task.toString())
        pb.start().waitFor()

        then:
        cas() == ['out.txt': hex('o\n'.bytes)]
    }
}
