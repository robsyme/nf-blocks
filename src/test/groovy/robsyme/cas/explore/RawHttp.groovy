package robsyme.cas.explore

/**
 * HTTP/1.1 over a plain socket, so a test can send the Host and Origin headers
 * a browser on another origin, or a DNS-rebinding page, would send. The JDK
 * HttpClient refuses to set Host.
 */
class RawHttp {

    static class Response {
        int status
        Map<String, String> headers
        byte[] body
        String text() { new String(body, 'UTF-8') }
    }

    static Response send(int port, String method, String rawPath, Map<String, String> headers = [:]) {
        final Map<String, String> all = [Host: "127.0.0.1:${port}".toString()] + headers
        new Socket('127.0.0.1', port).withCloseable { Socket s ->
            final String head = "${method} ${rawPath} HTTP/1.1\r\n" +
                all.collect { k, v -> "${k}: ${v}\r\n" }.join('') + 'Connection: close\r\n\r\n'
            s.outputStream.write(head.getBytes('ISO-8859-1'))
            s.outputStream.flush()
            final byte[] raw = s.inputStream.readAllBytes()
            final String text = new String(raw, 'ISO-8859-1')
            final int split = text.indexOf('\r\n\r\n')
            final List<String> lines = text.substring(0, split).split('\r\n') as List<String>
            final Map<String, String> parsed = [:]
            lines.drop(1).each { String line ->
                final int colon = line.indexOf(':')
                parsed[line.substring(0, colon).trim().toLowerCase()] = line.substring(colon + 1).trim()
            }
            return new Response(status: lines[0].split(' ')[1] as int, headers: parsed,
                body: Arrays.copyOfRange(raw, split + 4, raw.length))
        }
    }
}
