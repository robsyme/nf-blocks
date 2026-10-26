package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Stream

import groovy.json.JsonException
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic

/** `<member>/nf/` on a local member. */
@CompileStatic
class LocalLineageRecords implements LineageRecords {

    static final String DATA = '.data.json'

    private final Path nf

    LocalLineageRecords(Path memberRoot) {
        this.nf = memberRoot.resolve('nf')
    }

    @Override
    List<String> keysUnder(String prefix) {
        final String dir = (prefix ?: '').replaceAll('/+$', '')
        if( !safe(dir) ) return []
        final Path start = dir ? nf.resolve(dir) : nf
        if( !Files.isDirectory(start) ) return []
        final List<String> keys = []
        final Stream<Path> walk = Files.walk(start)
        try {
            walk.filter { Path p -> p.fileName.toString() == DATA && Files.isRegularFile(p) }.forEach { Path p ->
                final String key = nf.relativize(p.parent).toString().replace(File.separatorChar, (char) '/')
                if( key && !(key == dir) ) keys.add(key)
            }
        }
        finally {
            walk.close()
        }
        Collections.sort(keys)
        return keys
    }

    @Override
    Map read(String key) {
        if( !key || !safe(key) ) return null
        final Path file = nf.resolve(key).resolve(DATA)
        if( !Files.isRegularFile(file) ) return null
        return file.withInputStream { InputStream is -> decode(is, key) }
    }

    /**
     * A key or prefix that stays under nf/: relative, no `..` segment and no
     * backslash. Keys come from block content (a RunManifest's `nf_run_hash`),
     * and `nf.resolve('/x')` is `/x`.
     */
    private static boolean safe(String key) {
        if( key.startsWith('/') || key.contains('\\') || (key.length() > 1 && key.charAt(1) == (char) ':') )
            return false
        return !key.split('/').any { String s -> s == '..' }
    }

    /** One `.data.json` as a Map, or an IllegalStateException that names the record's key. */
    static Map decode(InputStream input, String key) {
        final Object parsed
        try {
            parsed = new JsonSlurper().parse(input, 'UTF-8')
        }
        catch( JsonException e ) {
            throw new IllegalStateException("nf record '${key}' is not readable JSON: ${e.message}", e)
        }
        if( !(parsed instanceof Map) )
            throw new IllegalStateException("nf record '${key}' is not a JSON object")
        return (Map) parsed
    }
}
