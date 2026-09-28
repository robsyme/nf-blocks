package robsyme.cas

import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import nextflow.exception.AbortRunException

/**
 * This machine's clock is more than 5 minutes from S3's (ticket 03 decision 2).
 * An {@link AbortRunException}, so a run stops with the message and no stack trace.
 */
@CompileStatic
@InheritConstructors
class ClockSkewException extends AbortRunException {
}
