package robsyme.cas.core

import groovy.json.JsonSlurper
import groovy.transform.Canonical
import groovy.transform.CompileStatic

/** The live/ registrations of a member, judged by the store's clock (ticket 20 answer 6). */
@CompileStatic
class LiveRegistry {

    @Canonical
    @CompileStatic
    static class Registration {
        String session
        long ageMillis
        String runName
        String pipeline
    }

    private final RetentionStorage storage

    LiveRegistry(RetentionStorage storage) { this.storage = storage }

    List<Registration> fresh() {
        return all().findAll { Registration r -> r.ageMillis <= SweepLock.STALE_MILLIS }.collect { Registration r -> withInfo(r) }
    }

    List<Registration> stale() { all().findAll { Registration r -> r.ageMillis > SweepLock.STALE_MILLIS } }

    void deleteStale() {
        for( Registration r : stale() )
            storage.deleteLive(r.session)
    }

    private Registration withInfo(Registration r) {
        try {
            final byte[] body = storage.readLive(r.session)
            if( body == null )
                return r
            final Map parsed = (Map) new JsonSlurper().parse(body)
            return new Registration(r.session, r.ageMillis, parsed.run_name as String, parsed.pipeline as String)
        }
        catch( Exception e ) {
            return r
        }
    }

    private List<Registration> all() {
        final List<Stamped> listed = storage.listLive()
        final long now = storage.nowMillis()
        return listed.collect { Stamped s -> new Registration(s.name, now - s.lastModifiedMillis, null, null) }
            .sort { Registration r -> r.session }
    }
}
