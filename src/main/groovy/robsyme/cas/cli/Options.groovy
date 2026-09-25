package robsyme.cas.cli

import groovy.transform.CompileStatic

/**
 * A verb's arguments as Nextflow's CmdPlugin hands them over: the positionals,
 * then each `--name value` as the pair `--name`, `value` (Launcher normalises
 * `--name value` to `--name=value`, CmdPlugin splits it again; DESIGN.md §15).
 */
@CompileStatic
class Options {

    final List<String> positionals
    private final Map<String, String> flags

    private Options(List<String> positionals, Map<String, String> flags) {
        this.positionals = Collections.unmodifiableList(positionals)
        this.flags = flags
    }

    static Options parse(List<String> args, Set<String> known) {
        final List<String> positionals = []
        final Map<String, String> flags = [:]
        for( int i = 0; i < (args ?: []).size(); i++ ) {
            final String arg = args[i]
            if( !arg.startsWith('--') ) {
                positionals << arg
                continue
            }
            final String name = arg.substring(2)
            if( !(name in known) )
                throw new UsageException("unknown option --${name}" + (known ? "; this verb takes ${known.collect { '--' + it }.join(', ')}" : '; this verb takes no options'))
            if( i + 1 >= args.size() )
                throw new UsageException("option --${name} needs a value")
            flags[name] = args[++i]
        }
        return new Options(positionals, flags)
    }

    String flag(String name) { flags.get(name) }

    int intFlag(String name, int fallback) {
        final String value = flags.get(name)
        if( value == null )
            return fallback
        if( !(value ==~ /\d+/) )
            throw new UsageException("option --${name} must be a whole number, got '${value}'")
        return Integer.parseInt(value)
    }
}
