# nf-blocks plugin 

> ⚠️ **IMPORTANT NOTICE** ⚠️
> 
> This plugin is currently under active development and is in an experimental state. It is **NOT** ready for production use. Features may change significantly, and there may be breaking changes without notice. Use at your own risk.

This project contains a Nextflow plugin called `nf-blocks` (formerly `nf-cid`) which provides content-addressed storage capabilities for Nextflow workflows. The plugin uses Content Identifiers (CIDs) to track and store workflow data, including:

- Task inputs and outputs
- Workflow run inputs and outputs
- Associated metadata

The plugin stores data blocks in `.nextflow/blocks` directory (relative to the workflow run location). Future versions will support multiple block stores, including remote storage systems like AWS S3 and Azure containers.

## Features

- Content-addressed storage using CIDs
- Local block storage in `.nextflow/blocks`
- CBOR encoding of task metadata
- DAG-CBOR block creation for task inputs

## Plugin structure
                    
- `settings.gradle`
    
    Gradle project settings. 

- `plugins/nf-blocks`
    
    The plugin implementation base directory.

- `plugins/nf-blocks/build.gradle` 
    
    Plugin Gradle build file. Project dependencies should be added here.

- `plugins/nf-blocks/src/resources/META-INF/MANIFEST.MF` 
    
    Manifest file defining the plugin attributes e.g. name, version, etc.

- `plugins/nf-blocks/src/resources/META-INF/extensions.idx`
    
    This file declares the extension classes provided by the plugin.

- `plugins/nf-blocks/src/main` 

    The plugin implementation sources.

- `plugins/nf-blocks/src/test` 

    The plugin unit tests. 

## Plugin classes

- `BlockstorePlugin`: the plugin entry point
- `BlockstoreObserver`: handles workflow events and manages content addressing
- `BlockstoreExtension`: provides content-addressed storage functionality to pipeline scripts
- `IPLDNode`: handles IPLD data structures and DAG-CBOR encoding (coming soon)
- `cbor/*`: CBOR encoding/decoding implementation for DAG-CBOR blocks

## Unit testing 

To run the unit tests:

```bash
make test
```

## Development setup

To build and test the plugin during development, configure a local Nextflow build with the following steps:

1. Clone the Nextflow repository in your computer into a sibling directory:
    ```bash
    git clone --depth 1 https://github.com/nextflow-io/nextflow ../nextflow
    ```
  
2. Configure the plugin build to use the local Nextflow code:
    ```bash
    echo "includeBuild('../nextflow')" >> settings.gradle
    ```
  
   (Make sure to not add it more than once!)

3. Compile the plugin alongside the Nextflow code:
    ```bash
    make assemble
    ```

4. Run Nextflow with the plugin:
    ```bash
    ./launch.sh run <pipeline> -plugins nf-blocks
    ```

## Testing without Nextflow build

The plugin can be tested without using a local Nextflow build:

1. Build the plugin: `make buildPlugins`
2. Copy `build/plugins/nf-blocks` to `$HOME/.nextflow/plugins`
3. Create a pipeline and run it: `nextflow run ./pipeline.nf`

## Package, upload, and publish

1. Create a `gradle.properties` file in the project root with:
   * `github_organization`: GitHub organisation hosting the plugin
   * `github_username`: GitHub username
   * `github_access_token`: GitHub access token
   * `github_commit_email`: GitHub email address

2. Package and create a release:
    ```bash
    ./gradlew :plugins:nf-blocks:upload
    ```

3. Create a pull request against [nextflow-io/plugins](https://github.com/nextflow-io/plugins/blob/main/plugins.json) to publish the plugin.
