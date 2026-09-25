package robsyme.cas.cli

import groovy.transform.CompileStatic

/** A verb was called wrongly; the message says how to call it. Exit status 2. */
@CompileStatic
class UsageException extends RuntimeException {
    UsageException(String message) { super(message) }
}
