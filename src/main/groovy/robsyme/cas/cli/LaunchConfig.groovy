package robsyme.cas.cli

import java.nio.file.Files
import java.nio.file.Path

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.config.ConfigBuilder
import nextflow.config.ConfigParser
import nextflow.config.ConfigParserFactory
import nextflow.secret.SecretsLoader

/**
 * The Nextflow config a plugin verb reads when Nextflow hands it no
 * {@code Launcher} (DESIGN.md §15): from 26.08.0-edge (nextflow 1dc8cf68f,
 * "Separate CLI from runtime") {@code CmdPlugin} calls
 * {@code exec(pluginId, cmd, args)} and passes none of the launcher's
 * options, so {@code -c}/{@code -C} cannot reach the plugin. What is read is
 * what {@code ConfigCmdAdapter.resolveConfigFiles} reads when no config file
 * is given (nf-cli-v1 at 1dc8cf68f): {@code <home>/config}, then
 * {@code <launch dir>/nextflow.config} (or {@code $NXF_CONFIG_FILE}), each
 * only when it exists, later files overriding earlier ones, with the
 * {@code standard} profile.
 *
 * Built only from runtime classes whose signatures are the same at 26.04.6
 * and 26.09.x-edge ({@code ConfigParserFactory.create}, {@code ConfigParser},
 * {@code ConfigBuilder.getConfigVars}, {@code SecretsLoader.secretContext}),
 * since the plugin compiles against 26.04.6.
 */
@Slf4j
@CompileStatic
class LaunchConfig {

    static final String DEFAULT_FILE = 'nextflow.config'

    /** The config files read, in order: those of the given paths that exist. */
    static List<Path> files(Path homeDir, Path launchDir, Map<String, String> env) {
        final List<Path> result = new ArrayList<Path>()
        final Path home = homeDir.resolve('config')
        if( Files.exists(home) )
            result.add(home)
        final Path local = launchDir.resolve(env.get('NXF_CONFIG_FILE') ?: DEFAULT_FILE)
        if( Files.exists(local) && local != home )
            result.add(local)
        return result
    }

    /**
     * @param homeDir   Nextflow's home, {@code Const.APP_HOME_DIR} ({@code $NXF_HOME} or {@code ~/.nextflow})
     * @param launchDir the directory the verb is run in
     * @param env       the environment, bound in the config as {@code ConfigBuilder} binds it
     * @return the merged config as plain nested maps; empty when no file exists
     */
    static Map read(Path homeDir, Path launchDir, Map<String, String> env) {
        final Path base = launchDir.toAbsolutePath().normalize()
        return read(files(homeDir.toAbsolutePath().normalize(), base, env), base, env)
    }

    /** Reads the given files (as {@link #files} lists them) over the launch directory {@code base}. */
    static Map read(List<Path> files, Path base, Map<String, String> env) {
        final ConfigObject merged = new ConfigObject()
        if( files.isEmpty() )
            return merged
        final Map binding = new HashMap(env)
        binding.putAll(ConfigBuilder.getConfigVars(base, SecretsLoader.secretContext()))
        final ConfigParser parser = ConfigParserFactory.create()
            .setBinding(binding)
            .setProfiles([ConfigBuilder.DEFAULT_PROFILE])
        for( Path file : files ) {
            log.debug("nf-blocks: reading config file ${file}")
            merged.merge(parser.parse(file))
        }
        return toMap(merged)
    }

    private static Map toMap(Map config) {
        final Map<String, Object> out = new LinkedHashMap<String, Object>()
        for( Map.Entry e : (Set<Map.Entry>) config.entrySet() )
            out.put(String.valueOf(e.key), e.value instanceof Map ? toMap((Map) e.value) : e.value)
        return out
    }
}
