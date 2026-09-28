package robsyme.cas

import nextflow.config.spec.ScopeName
import nextflow.config.spec.SpecNode
import spock.lang.Specification

/**
 * The scope exists only so {@code ConfigValidator} recognises {@code cas.*}
 * keys instead of warning on each one (DESIGN.md §2). The validator builds a
 * {@link SpecNode.Scope} from the class by reflection, so proving the options
 * resolve there is proving the warnings are silenced, without a pipeline run.
 */
class CasConfigScopeTest extends Specification {

    def 'is annotated cas'() {
        expect:
        CasConfigScope.getAnnotation(ScopeName).value() == 'cas'
    }

    def 'declares the options the validator must recognise'() {
        given:
        final scope = SpecNode.Scope.of(CasConfigScope, '')

        expect: 'the flat options'
        scope.getOption(['resolve']) != null
        scope.getOption(['asserted_by']) != null
        scope.getOption(['pipeline']) != null
        scope.getOption(['tmpDir']) != null
        scope.getOption(['nodeHash']) != null

        and: 'the nested index scope'
        scope.getOption(['index', 'path']) != null

        and: 'a placeholder store member: cas.stores.<alias>.location'
        scope.getOption(['stores', 'lab', 'location']) != null
    }
}
