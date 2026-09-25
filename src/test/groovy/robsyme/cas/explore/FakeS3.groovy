package robsyme.cas.explore

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer

/**
 * Just enough of S3's REST API, path style, for S3MemberFiles through the real
 * SDK: HeadObject, GetObject with one Range, ListObjectsV2 with paging.
 */
class FakeS3 {

    final Map<String, byte[]> objects = [:]      // "bucket/key" -> bytes
    final List<String> requests = []
    int pageSize = 1000
    private HttpServer server

    FakeS3 start() {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0)
        server.createContext('/') { HttpExchange ex -> handle(ex) }
        server.start()
        return this
    }

    String getUrl() { "http://127.0.0.1:${server.address.port}" }

    void stop() { server?.stop(0) }

    private void handle(HttpExchange ex) {
        try {
            final String path = URLDecoder.decode(ex.requestURI.rawPath.substring(1), 'UTF-8')
            final Map<String, String> query = (ex.requestURI.rawQuery ?: '').split('&').findAll().collectEntries {
                final List<String> kv = it.split('=', 2) as List<String>
                [(URLDecoder.decode(kv[0], 'UTF-8')): kv.size() > 1 ? URLDecoder.decode(kv[1], 'UTF-8') : '']
            }
            requests << "${ex.requestMethod} /${path}${query ? '?' + query.keySet().sort().join('&') : ''}".toString()
            if( query['list-type'] == '2' )
                list(ex, path.replaceAll('/$', ''), query)
            else
                object(ex, path)
        }
        finally {
            ex.close()
        }
    }

    private void object(HttpExchange ex, String path) {
        final byte[] bytes = objects[path]
        if( bytes == null ) {
            final byte[] body = '<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>no</Message></Error>'.bytes
            ex.responseHeaders.set('Content-Type', 'application/xml')
            ex.sendResponseHeaders(404, ex.requestMethod == 'HEAD' ? -1 : body.length)
            if( ex.requestMethod != 'HEAD' ) ex.responseBody.write(body)
            return
        }
        ex.responseHeaders.set('Content-Type', 'application/octet-stream')
        ex.responseHeaders.set('Accept-Ranges', 'bytes')
        ex.responseHeaders.set('ETag', '"fake"')
        final String range = ex.requestHeaders.getFirst('Range')
        if( ex.requestMethod == 'HEAD' ) {
            ex.responseHeaders.set('Content-Length', String.valueOf(bytes.length))
            ex.sendResponseHeaders(200, -1)
            return
        }
        if( range ) {
            final def m = range =~ /^bytes=(\d+)-(\d+)$/
            m.matches()
            final int start = m.group(1) as int
            final int end = Math.min(m.group(2) as int, bytes.length - 1)
            ex.responseHeaders.set('Content-Range', "bytes ${start}-${end}/${bytes.length}")
            ex.sendResponseHeaders(206, end - start + 1)
            ex.responseBody.write(bytes, start, end - start + 1)
            return
        }
        ex.sendResponseHeaders(200, bytes.length)
        ex.responseBody.write(bytes)
    }

    private void list(HttpExchange ex, String bucket, Map<String, String> query) {
        final String prefix = query['prefix'] ?: ''
        final List<String> keys = objects.keySet().findAll { it.startsWith("${bucket}/${prefix}") }
            .collect { it.substring(bucket.length() + 1) }.sort()
        final int from = (query['continuation-token'] ?: '0') as int
        final List<String> page = keys.drop(from).take(pageSize)
        final boolean more = from + page.size() < keys.size()
        final String xml = '<?xml version="1.0" encoding="UTF-8"?>' +
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
            "<Name>${bucket}</Name><Prefix>${prefix}</Prefix><KeyCount>${page.size()}</KeyCount>" +
            "<MaxKeys>${pageSize}</MaxKeys><IsTruncated>${more}</IsTruncated>" +
            (more ? "<NextContinuationToken>${from + page.size()}</NextContinuationToken>" : '') +
            page.collect { "<Contents><Key>${it}</Key><Size>${objects["${bucket}/${it}"].length}</Size></Contents>" }.join('') +
            '</ListBucketResult>'
        final byte[] body = xml.getBytes('UTF-8')
        ex.responseHeaders.set('Content-Type', 'application/xml')
        ex.sendResponseHeaders(200, body.length)
        ex.responseBody.write(body)
    }
}
