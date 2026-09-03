package robsyme.cas.lineage

import groovy.transform.CompileStatic
import nextflow.lineage.LinStore
import nextflow.lineage.LinStoreFactory
import nextflow.lineage.config.LineageConfig
import nextflow.plugin.Priority

/**
 * Claims `lineage.store.location = 'cas://<alias>'` for the content-addressed store.
 *
 * `canOpen` is called for every lineage-enabled run before any other factory is
 * asked, so it must answer for a config this plugin knows nothing about,
 * including one with no `store.location` at all (DESIGN.md section 2).
 */
@CompileStatic
@Priority(-10)
class CasLinStoreFactory extends LinStoreFactory {

    /** Widened from `protected` to public, as the reference implementations do. */
    @Override
    boolean canOpen(LineageConfig config) {
        return config?.store?.location?.startsWith('cas://') ?: false
    }

    @Override
    protected LinStore newInstance(LineageConfig config) {
        return new CasLinStore().open(config)
    }
}
