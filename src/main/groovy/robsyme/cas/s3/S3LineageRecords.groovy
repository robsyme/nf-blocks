package robsyme.cas.s3

import groovy.transform.CompileStatic
import robsyme.cas.core.LineageRecords
import robsyme.cas.core.LocalLineageRecords

/** `<prefix>nf/` on an S3 member, over the same S3Ops seam as the rest of the member. */
@CompileStatic
class S3LineageRecords implements LineageRecords {

    private static final String DATA = '/.data.json'

    private final S3Ops ops
    private final String root

    S3LineageRecords(S3Ops ops, String prefix) {
        this.ops = ops
        this.root = "${prefix ?: ''}nf/".toString()
    }

    @Override
    List<String> keysUnder(String prefix) {
        final String dir = (prefix ?: '').replaceAll('/+$', '')
        final String under = dir ? root + dir + '/' : root
        final List<String> keys = []
        for( S3Listed o : ops.list(under, 0) ) {
            if( !o.key.endsWith(DATA) ) continue
            final String key = o.key.substring(root.length(), o.key.length() - DATA.length())
            if( key && key != dir ) keys.add(key)
        }
        // S3 lists by the whole object key, so 'a.b/.data.json' comes before 'a/.data.json'.
        Collections.sort(keys)
        return keys
    }

    @Override
    Map read(String key) {
        if( !key ) return null
        final InputStream in = ops.get(root + key + DATA, null, 0L, -1L)
        if( in == null ) return null
        return in.withCloseable { InputStream is -> LocalLineageRecords.decode(is, key) }
    }
}
