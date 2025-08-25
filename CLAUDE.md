# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `nf-blocks`, a Nextflow plugin that provides content-addressed storage capabilities using Content Identifiers (CIDs). The plugin implements content-addressed block storage for Nextflow workflows, supporting both local filesystem and IPFS backends.

**⚠️ IMPORTANT**: This plugin is experimental and not production-ready. Features may change without notice.

## Build Commands

The project uses Gradle with a Makefile wrapper:

- `make test` - Run unit tests
- `make assemble` - Build the plugin
- `make clean` - Clean build artifacts and temporary files
- `make install` - Install plugin to local Nextflow plugins directory
- `make release` - Publish the plugin

Direct Gradle commands:
- `./gradlew test` - Run tests
- `./gradlew assemble` - Build plugin
- `./gradlew clean` - Clean build

## Development Setup

For local development with Nextflow:

1. Clone Nextflow into sibling directory: `git clone --depth 1 https://github.com/nextflow-io/nextflow ../nextflow`
2. Configure local build: `echo "includeBuild('../nextflow')" >> settings.gradle`
3. Build and install plugin: `make install` (installs to local Nextflow plugins directory)
4. Run with plugin: `./launch.sh run <pipeline> -plugins nf-blocks`

The `launch.sh` script sets `NXF_PLUGINS_DEV` to use the local plugin build.

**Note**: After making changes to the plugin code, you need to run `make install` to rebuild and install the updated plugin before testing.

## Implementation Status

This plugin provides **dual functionality**: a filesystem provider for storing files in block stores, and content-addressed metadata tracking for published workflow outputs.

### Phase 1: Enhanced URI Scheme ✅ COMPLETED
- **Self-Contained URIs**: Block store backend and configuration specified directly in URI
- **Multiple Backends**: Support for `blocks+file://` and `blocks+http://` schemes  
- **No Configuration**: Eliminates need for `nextflow.config` block store configuration
- **Dynamic Creation**: Block stores created automatically from URI specification

### Phase 2: Publication Tracking ✅ WORKING  
- **CID Recording**: Files are published and CIDs are generated and logged
- **Block Persistence**: Blocks correctly stored in local filesystem or IPFS
- **Content Addressing**: All published content becomes addressable by CID

### Phase 3: Global Filesystem Root CID ⚠️ IN PROGRESS
**Current Status**: Write-only publication system implemented with basic directory folding, but only tracks individual directory roots separately.

**Goal**: Implement a global filesystem root that starts as an empty DAG-PB directory and grows incrementally with each publication event, maintaining a single root CID representing the entire published dataset.

#### Implementation Plan:

**Step 1: Empty Root Creation**
- Create initial DAG-PB empty directory structure
- Store as the initial "filesystem root CID" 
- This becomes the baseline for all subsequent publications

**Step 2: Path-Aware Folding Algorithm**
- Parse publication target paths to understand directory hierarchy (e.g., `/results/sample1/data.txt`)
- Build nested directory structure by creating intermediate DAG-PB directory nodes
- Handle deep paths: `/a/b/c/file.txt` requires creating directories for `a`, `a/b`, and `a/b/c`

**Step 3: Directory Tree Merging**
- Load current filesystem root DAG-PB node
- Navigate/create directory tree to target location
- Merge new file/directory into the correct location within the tree
- Rebuild all affected ancestor directories up to root
- Calculate and return new root CID

**Step 4: Complex Scenarios**
- **Multiple files in same directory**: `/results/sample1/file1.txt` + `/results/sample1/file2.txt`
- **Overlapping directories**: `/results/sample1/data.txt` + `/results/sample2/data.txt`
- **Deep nested paths**: `/results/analysis/2024/batch1/sample1/output.txt`
- **File replacement**: Publishing to same path should replace existing file

**Data Structures Needed**:
```groovy
class FilesystemRoot {
    String rootCid                    // Current root CID of entire filesystem
    BlockStore blockStore            // Where to store DAG-PB nodes
    
    String addFile(String path, String contentCid)
    String addDirectory(String path, String directoryCid)
    DagPbNode loadDirectory(String cid)
    String createDirectory(Map<String, String> entries)  
}
```

**Algorithm Complexity**:
- Path parsing and validation
- Directory tree navigation/creation
- DAG-PB node construction for nested directories
- Incremental updates without full filesystem reconstruction
- Proper link ordering as required by DAG-PB specification

### Phase 4: Metadata Collections (Future)
- **DAG-CBOR Metadata**: Create content-addressed collections of workflow metadata
- **CID Joining**: Join workflow metadata with published content CIDs
- **Publication Events**: Implement via publication event watchers

This approach uses **enhanced URI schemes** to eliminate configuration complexity while providing flexible, self-contained block storage URIs.

## Architecture

### Core Components

- **BlocksPlugin** (`src/main/groovy/nextflow/blocks/BlocksPlugin.groovy`) - Main plugin entry point
- **BlocksExtension** (`src/main/groovy/nextflow/blocks/BlocksExtension.groovy`) - Provides DSL extensions and channel operations for Nextflow scripts
- **BlocksObserver** (`src/main/groovy/nextflow/blocks/BlocksObserver.groovy`) - Handles workflow events and content addressing (currently mostly commented out)
- **BlocksFactory** - Extension point factory for plugin registration

### Block Store Implementations

The plugin supports multiple block storage backends:

- **LocalBlockStore** (`src/main/groovy/nextflow/blocks/LocalBlockStore.groovy`) - Local filesystem storage with UnixFS support
- **IpfsBlockStore** (`src/main/groovy/nextflow/blocks/IpfsBlockStore.groovy`) - IPFS backend integration

### File System Integration

- **BlocksFileSystemProvider** (`src/main/groovy/nextflow/blocks/fs/BlocksFileSystemProvider.groovy`) - Java NIO file system provider
- **BlocksFileSystem** and related classes - Complete NIO file system implementation for content-addressed paths

### Protocol Support

- **DagPB codec** (`src/main/groovy/nextflow/blocks/dagpb/`) - DAG-PB (Directed Acyclic Graph Protocol Buffers) implementation
- **UnixFS codec** (`src/main/groovy/nextflow/blocks/unixfs/`) - UnixFS file system protocol implementation
- **Protocol Buffers** (`src/main/proto/`) - `.proto` files for DAG-PB and UnixFS specifications

## Enhanced URI Scheme

The plugin uses **enhanced URI schemes** that encode backend type and configuration directly in the URI, eliminating the need for separate configuration.

### Supported URI Schemes

#### Local Filesystem Backend
```groovy
// Basic usage - creates block store at ./store-name/
outputDir = 'blocks+file:///store-name/output-path'

// Examples:
outputDir = 'blocks+file:///blocks-store/results'     // → ./blocks-store/ 
outputDir = 'blocks+file:///my-blocks/data'           // → ./my-blocks/
```

#### HTTP/IPFS Backend  
```groovy
// IPFS node via HTTP API
outputDir = 'blocks+http://127.0.0.1:5001/output-path'

// Examples:
outputDir = 'blocks+http://127.0.0.1:5001/'          // Default IPFS port
outputDir = 'blocks+http://ipfs-node:8080/data'      // Custom IPFS endpoint
```

### URI Structure

```
blocks+<backend>://<backend-config>/<path-in-blocks>
  │       │              │               │
  │       │              │               └─ Path within blocks filesystem
  │       │              └─ Backend-specific configuration  
  │       └─ Backend type (file, http, s3*, etc.)
  └─ Protocol identifier

* Future backends like S3 will follow the same pattern
```

### No Configuration Required

Unlike the old approach, **no `nextflow.config` setup is needed**:

```groovy
// Old approach (no longer supported):
// blocks { store { type = 'local'; path = './blocks' } }

// New approach - everything in the URI:
outputDir = 'blocks+file:///blocks-store/results'
```

## Content Addressing

The plugin uses **Content Identifiers (CIDs)** to address data in block stores. CIDs are self-describing identifiers that encode the hash algorithm, content type, and cryptographic hash of the data.

### Block Store Backends
- **LocalBlockStore**: Calculates SHA-256 hashes locally and stores blocks in the filesystem
- **IpfsBlockStore**: Delegates to IPFS daemon for hash calculation and storage

### Content Identifiers  
CIDs (e.g., `bafyreiawgxbrwagajds5w3aafcd56a24psy7rlrr7vhrdw3h4qwppbn7tq`) encode:
- **Version**: CIDv1 format
- **Codec**: Data format (dag-cbor, dag-pb, raw, etc.)
- **Hash Algorithm**: Cryptographic function used (typically SHA-256)
- **Hash Value**: Unique fingerprint of the content

This ensures that identical content always produces the same CID, regardless of storage backend.

## Testing

Tests are located in `src/test/groovy/nextflow/blocks/` and include:
- Unit tests for block store implementations
- File system provider tests
- Mock helpers for testing

## Key Dependencies

- **IPFS**: `com.github.ipfs:java-ipfs-http-client:1.4.4`
- **CID**: `com.github.ipld:java-cid:1.3.8` 
- **Protocol Buffers**: `com.google.protobuf:protobuf-java:3.24.0`
- **Nextflow**: Requires minimum version `25.01.0-edge`

## Plugin Registration

The plugin registers extension points in `build.gradle`:
- `nextflow.blocks.BlocksFactory`
- `nextflow.blocks.BlocksExtension`

The BlocksPlugin registers multiple FileSystemProviders with Java NIO to handle enhanced URI schemes (`blocks+file`, `blocks+http`, etc.).

## Working Examples

### Local Filesystem Example
```groovy
// nextflow.config
outputDir = 'blocks+file:///blocks-store/results'

// Run pipeline  
nextflow run pipeline.nf
```

**Result**: Files stored as blocks in `./blocks-store/` directory with CIDs like `bafybeigamvgwlniubwypzowrm4wazclfgvtzbe5svxld6emyikn4hctw7i`

### IPFS Example
```groovy  
// nextflow.config
outputDir = 'blocks+http://127.0.0.1:5001/'

// Run pipeline (requires IPFS node running on port 5001)
nextflow run pipeline.nf
```

**Result**: Files stored directly in IPFS with CIDs like `QmZawDn8mM1fYbW7wAM6tkNfYiuGgmW47bePAAjbmSvaXx`

### IPFS Block Retrieval
If a block was persisted to IPFS, you can retrieve the DAG-JSON representation:

```bash
# Get block content as DAG-JSON
ipfs dag get QmZawDn8mM1fYbW7wAM6tkNfYiuGgmW47bePAAjbmSvaXx

# Get block raw data  
ipfs block get QmZawDn8mM1fYbW7wAM6tkNfYiuGgmW47bePAAjbmSvaXx
```