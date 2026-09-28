package robsyme.cas.core

import java.nio.file.Files
import java.nio.file.Path

import groovy.transform.Canonical
import groovy.transform.CompileStatic

/**
 * The one seam through which a published file's address arrives (spec §3,
 * Address Providers). The implementation stores the content in the writable
 * member, or finds it already there, and says which provider supplied the address.
 */
@CompileStatic
interface FileAddresser {
    Addressed address(Path file, long size)
}

@Canonical
@CompileStatic
class Addressed {
    Cid cid
    long size
    String provider
}

/** The provider that always works: the head node streams the file through one hash buffer. */
@CompileStatic
class HeadNodeAddresser implements FileAddresser {
    private final BlockStore store
    HeadNodeAddresser(BlockStore store) { this.store = store }

    @Override
    Addressed address(Path file, long size) {
        final InputStream input = Files.newInputStream(file)
        try {
            return new Addressed(store.putStreaming(input), size, Providers.HEAD_NODE)
        }
        finally {
            input.close()
        }
    }
}
