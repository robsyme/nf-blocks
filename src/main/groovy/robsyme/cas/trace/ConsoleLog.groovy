package robsyme.cas.trace

import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * The plugin's lines meant for the person at the terminal. Nextflow's console
 * appender admits only loggers whose names start with a configured package,
 * {@code nextflow} among them (LoggerHelper.ConsoleLoggerFilter,
 * LoggerHelper.groovy:408-441 at v26.04.6), so a {@code robsyme.cas.*} logger
 * reaches {@code .nextflow.log} only. This one is named under {@code nextflow.}
 * and so reaches both. Every other plugin log stays file-only.
 */
@CompileStatic
final class ConsoleLog {

    static final String NAME = 'nextflow.cas'

    static final Logger LOG = LoggerFactory.getLogger(NAME)

    private ConsoleLog() {}
}
