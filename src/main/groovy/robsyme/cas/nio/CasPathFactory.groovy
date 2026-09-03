package robsyme.cas.nio

import java.nio.file.Path

import groovy.transform.CompileStatic
import nextflow.file.FileSystemPathFactory
import robsyme.cas.CasPlugin

/**
 * Turns `cas://` strings into paths and back (DESIGN.md section 8).
 *
 * `FileHelper.asPath0` consults the registered factories before it looks at the
 * installed file system providers, so this is the path Nextflow actually takes
 * for `outputDir = 'cas://lab'`.
 */
@CompileStatic
class CasPathFactory extends FileSystemPathFactory {

    private static final String PREFIX = "${CasPath.SCHEME}://"

    @Override
    protected Path parseUri(String uri) {
        if( !uri || !uri.startsWith(PREFIX) )
            return null
        return CasPlugin.provider().getPath(URI.create(uri))
    }

    @Override
    protected String toUriString(Path path) {
        return path instanceof CasPath ? path.toString() : null
    }

    /** No task-side staging in the skeleton: the local executor transfers on the head node. */
    @Override
    protected String getBashLib(Path target) { return null }

    @Override
    protected String getUploadCmd(String source, Path target) { return null }
}
