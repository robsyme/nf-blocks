package robsyme.cas.core

import java.security.SecureRandom
import java.util.concurrent.ConcurrentHashMap

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/** Ids of member incarnations (DESIGN.md §20 decisions 1 and 2). */
@Slf4j
@CompileStatic
class StoreIds {

    static final String FILE = 'store-id'
    private static final SecureRandom RANDOM = new SecureRandom()
    private static final Set<String> WARNED = ConcurrentHashMap.newKeySet()

    static String newId() {
        final byte[] bytes = new byte[16]
        RANDOM.nextBytes(bytes)
        final StringBuilder hex = new StringBuilder(32)
        for( byte b : bytes )
            hex.append(Integer.toHexString((b & 0xff) | 0x100).substring(1))
        return hex.toString()
    }

    static boolean valid(String id) { id != null && id ==~ /[0-9a-f]{32}/ }

    /** The id in a `store-id` body, or null (warned once per describe()) when it is not one. */
    static String parse(byte[] body, String where) {
        final String text = new String(body, 'UTF-8').trim()
        if( valid(text) )
            return text
        if( WARNED.add(where) )
            log.warn("ignoring ${where}: it does not hold a store id (32 lowercase hex characters)")
        return null
    }

    /** Reads the id, creating it when absent. */
    static String ensure(StoreIdStorage storage) {
        final String id = storage.read()
        return id != null ? id : storage.create(newId())
    }

    /** The member's id, or null when the store keeps none or has none. */
    static String read(BlockStore store) {
        return store instanceof IdentifiedStore ? ((IdentifiedStore) store).storeIdStorage().read() : null
    }
}
