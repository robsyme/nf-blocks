package robsyme.cas.trace

import groovy.transform.CompileStatic

/**
 * The warning for a script that calls {@code fromStore} where Nextflow cannot
 * reach it (tickets 06 and 07). Pure: it reads only the error.
 *
 * Nextflow 26.04.6 wraps the call's {@link MissingMethodException} in a
 * MissingProcessException (WorkflowDef.groovy:189-190). An untyped script's
 * {@code channel} is {@code nextflow.Channel}, whose missing-method hook throws
 * with method {@code Channel.fromStore} when no include loaded the factory
 * (PluginExtensionProvider.groovy:289). A typed script's {@code channel} is
 * {@code nextflow.dataflow.ChannelNamespace} and its {@code Channel} is
 * {@code nextflow.script.types.Channel}; neither consults plugin factories, so
 * Groovy's own error names method {@code fromStore} with or without the include.
 */
@CompileStatic
class FromStoreHint {

    static final String UNTYPED =
        "`fromStore` comes from the nf-blocks plugin: add `include { fromStore } from 'plugin/nf-blocks'` at the top of the script."

    static final String TYPED =
        "In a typed script, `channel.fromStore` can't be reached until nextflow-io/nextflow#7694 is fixed. " +
        "Add the include and call `nextflow.Channel.fromStore(...)`, with `records: true` for record-typed inputs."

    private static final String UNTYPED_METHOD = 'Channel.fromStore'
    private static final String TYPED_METHOD = 'fromStore'
    private static final Set<String> TYPED_RECEIVERS = Collections.unmodifiableSet(
        ['nextflow.dataflow.ChannelNamespace', 'nextflow.script.types.Channel'] as Set<String>)

    private FromStoreHint() {}

    /** The warning for {@code error}, or null when it is not a missing {@code fromStore}. */
    static String of(Throwable error) {
        final MissingMethodException missing = firstMissingMethod(error)
        if( missing == null )
            return null
        if( missing.method == UNTYPED_METHOD )
            return UNTYPED
        if( missing.method == TYPED_METHOD && TYPED_RECEIVERS.contains(missing.type?.name) )
            return TYPED
        return null
    }

    private static MissingMethodException firstMissingMethod(Throwable error) {
        final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>())
        Throwable current = error
        while( current != null && seen.add(current) ) {
            if( current instanceof MissingMethodException )
                return (MissingMethodException) current
            current = current.cause
        }
        return null
    }
}
