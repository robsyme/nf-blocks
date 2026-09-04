package robsyme.cas.trace

import groovy.transform.CompileStatic
import nextflow.Session
import nextflow.trace.TraceObserverV2
import nextflow.trace.TraceObserverFactoryV2

/**
 * Registers the one {@link CasObserver} of a run (DESIGN.md §11). Listed in
 * {@code extensionPoints}; {@code @Extension} alone registers nothing.
 */
@CompileStatic
class CasObserverFactory implements TraceObserverFactoryV2 {

    @Override
    Collection<TraceObserverV2> create(Session session) {
        return Collections.<TraceObserverV2> singletonList(new CasObserver())
    }
}
