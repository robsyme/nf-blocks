package robsyme.cas.trace

import nextflow.dataflow.ChannelNamespace
import nextflow.exception.MissingProcessException
import nextflow.exception.ScriptCompilationException
import nextflow.script.ScriptMeta
import nextflow.script.WorkflowBinding
import nextflow.script.types.Channel as TypedChannel
import spock.lang.Specification

/**
 * The chains measured on Nextflow 26.04.6 (research: missing-fromstore-exception-chain):
 * a MissingProcessException, wrapped by WorkflowDef.run around the
 * MissingMethodException the call raised.
 */
class FromStoreHintTest extends Specification {

    private static final Object[] ARGS = [[selection: 'bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua']] as Object[]

    /** What WorkflowDef.groovy:189-190 throws around the call's own exception. */
    private MissingProcessException wrapped(MissingMethodException cause) {
        final ScriptMeta meta = Stub(ScriptMeta) {
            getAllNames() >> new HashSet<String>()
        }
        return new MissingProcessException(meta, cause)
    }

    def 'an untyped channel.fromStore with no include gets the include line'() {
        given: 'PluginExtensionProvider.groovy:289'
        final error = wrapped(new MissingMethodException('Channel.fromStore', Object, ARGS))

        expect:
        FromStoreHint.of(error) == FromStoreHint.UNTYPED
        FromStoreHint.UNTYPED == "`fromStore` comes from the nf-blocks plugin: add `include { fromStore } from 'plugin/nf-blocks'` at the top of the script."
    }

    def 'a typed #call gets the nextflow.Channel workaround'() {
        given:
        final error = wrapped(new MissingMethodException('fromStore', receiver, ARGS, true))

        expect:
        FromStoreHint.of(error) == FromStoreHint.TYPED
        FromStoreHint.TYPED == "In a typed script, `channel.fromStore` can't be reached until nextflow-io/nextflow#7694 is fixed. " +
            "Add the include and call `nextflow.Channel.fromStore(...)`, with `records: true` for record-typed inputs."

        where:
        call                  | receiver
        'channel.fromStore'   | ChannelNamespace
        'Channel.fromStore'   | TypedChannel
    }

    def 'the MissingMethodException is found unwrapped, and under more than one wrapper'() {
        expect:
        FromStoreHint.of(new MissingMethodException('Channel.fromStore', Object, ARGS)) == FromStoreHint.UNTYPED
        FromStoreHint.of(new RuntimeException('outer', wrapped(new MissingMethodException('fromStore', ChannelNamespace, ARGS, true)))) == FromStoreHint.TYPED
    }

    def 'no hint for #why'() {
        expect:
        FromStoreHint.of(error) == null

        where:
        why                                              | error
        'no error'                                       | null
        'another missing channel factory'                | new MissingMethodException('Channel.fromLineage', Object, ARGS)
        'another missing typed member'                   | new MissingMethodException('fromPathz', ChannelNamespace, ARGS, true)
        'a bare fromStore call (WorkflowBinding:115)'    | new MissingMethodException('fromStore', WorkflowBinding, ARGS)
        'a compile error'                                | new ScriptCompilationException('Script compilation failed')
        'a task failure'                                 | new IllegalStateException('Process `HASH` terminated with an error exit status (1)')
    }

    def 'no hint for an unrelated missing method wrapped the same way'() {
        expect:
        FromStoreHint.of(wrapped(new MissingMethodException('HASHH', Object, ARGS))) == null
    }

    def 'a cause chain that loops ends the walk'() {
        given:
        final RuntimeException a = new RuntimeException('a')
        final RuntimeException b = new RuntimeException('b')
        a.initCause(b)
        b.initCause(a)

        expect:
        FromStoreHint.of(a) == null
    }
}
