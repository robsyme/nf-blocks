/*
 * Copyright 2025, Rob Syme (rob.syme@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package robsyme.plugin

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.plugin.BasePlugin
import nextflow.file.FileHelper
import org.pf4j.PluginWrapper
import robsyme.plugin.fs.*

/**
 * The Blocks plugin entry point
 *
 * Implements content-addressed output storage with IPLD manifests
 * supporting S3 and local file storage backends.
 */
@Slf4j
@CompileStatic
class BlocksPlugin extends BasePlugin {

    BlocksPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }

    @Override
    void start() {
        super.start()
        log.info "Blocks plugin started - version ${wrapper.descriptor.version}"

        // Register filesystem providers for blocks+<scheme> URIs
        // This allows outputDir = 'blocks+file://...', 'blocks+s3://...', etc.
        FileHelper.getOrInstallProvider(BlocksFileFileSystemProvider)
        FileHelper.getOrInstallProvider(BlocksS3FileSystemProvider)

        log.debug "Registered Blocks filesystem providers: blocks+file, blocks+s3"
    }

    @Override
    void stop() {
        log.info "Blocks plugin stopped"
        super.stop()
    }
}
