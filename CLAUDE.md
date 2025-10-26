# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

nf-blocks is a Nextflow plugin that implements content-addressed storage for workflow outputs using IPLD (InterPlanetary Linked Data) with BLAKE3 hashing. The plugin enables reproducible, queryable, and shareable computational results with cryptographic verification.

**Core concepts:**
- **Content-addressed storage**: Files stored by their BLAKE3 hash as IPLD CIDv1 identifiers (e.g., `bafkr4i...`)
- **Streaming uploads**: Hash calculation happens in parallel with file I/O using `b3sum` CLI process
- **Multi-backend support**: Works with local filesystems and S3
- **IPLD manifests**: Run metadata and provenance stored as DAG-CBOR objects
- **Git-like refs**: Mutable pointers (e.g., `refs/workflows/main/latest`) to immutable content

## Build and Development Commands

```bash
# Build the plugin (compile and package)
make assemble

# Run unit tests
make test

# Install plugin to local Nextflow plugins directory
# IMPORTANT: Always run this after code changes before testing
make install

# Clean build artifacts and Nextflow work directories
make clean

# Test with example workflow (after make install)
cd examples
nextflow run . -profile local

# Publish plugin (requires credentials)
make release
```

## Testing Workflow

**Always follow this sequence when making changes:**

1. Make code changes
2. Run `make install` from project root
3. Test with example workflow: `cd examples && nextflow run . -profile local`
4. Verify results in `/tmp/nf-blocks/blobs/` (should see CID-named files)
5. Clean up between test runs: `rm -rf .nextflow* work /tmp/nf-blocks`

**Unit tests:** Run `make test` or `./gradlew test --tests "ClassName.testMethod"` for specific tests.

## Architecture Overview

### Plugin Entry Point
**BlocksPlugin** registers filesystem providers and initializes the plugin. It uses Nextflow's `FileHelper.getOrInstallProvider()` for programmatic registration (not META-INF/services).

### Filesystem Providers (blocks+<scheme> URIs)
The plugin intercepts writes to URIs like `blocks+file://` and `blocks+s3://` through a provider hierarchy:

- **BlocksFileSystemProvider** (base class): Parses `blocks+<scheme>` URIs and delegates to underlying filesystem
- **BlocksFileFileSystemProvider**: Handles `blocks+file://` (local filesystem)
- **BlocksS3FileSystemProvider**: Handles `blocks+s3://` (S3 storage)

**Critical:** Subclasses only override `getScheme()`. All delegation logic is in the base class.

**Note:** Azure Blob Storage and Google Cloud Storage support are not yet implemented.

### Streaming Upload Pipeline

Files written to `blocks+<scheme>` URIs go through content-addressed storage:

**LocalBlocksOutputStream** (local filesystems):
1. Write to temp file: `{basePath}/.staging/{uuid}.tmp`
2. Stream data simultaneously to file and `b3sum` process
3. On close: read BLAKE3 hash, convert to CID, atomic move to `{basePath}/blobs/{cid}`

**S3MultipartBlocksOutputStream** (S3):
1. Multipart upload to `.staging/{uuid}` with 5MB parts
2. Stream data simultaneously to S3 and `b3sum` process
3. On close: CompleteMultipartUpload, then server-side CopyObject to `blobs/{cid}`

**Key implementation detail:** Both use `ProcessBuilder` to spawn `b3sum --no-names` CLI process for streaming BLAKE3 calculation.

### TraceObserver Pattern

**BlocksObserver** implements Nextflow's `TraceObserver` interface to intercept workflow execution:

- `onFlowCreate()`: Initialize storage backends (BlobStore, RefStore)
- `onProcessComplete()`: Upload task outputs, create OutputMetadata objects
- `onFlowComplete()`: Build RunManifest, update refs (e.g., `refs/workflows/{name}/latest`)

### Storage Abstraction

Two-tier storage architecture:

**BlobStore** (content-addressed data):
- `FileBlobStore`: Local filesystem blobs
- `S3BlobStore`: S3 blobs

**RefStore** (mutable references):
- `FileRefStore`: Local filesystem refs
- `S3RefStore`: S3 refs (small text files with CIDs)

**StorageBackendFactory** creates appropriate implementations based on URI scheme.

### IPLD Data Structures

**CID.groovy**: Converts BLAKE3 hashes to IPLD CIDv1 format
- Format: `bafkr4i{base32-encoded-multihash}`
- Codec: 0x55 (raw) for data blobs, 0x71 (dag-cbor) for manifests
- Hash: 0x1e (BLAKE3), 32 bytes

**OutputMetadata**: Per-file metadata with sample info, process details, and data blob link
**RunManifest**: Workflow-level metadata linking all outputs with provenance chain
**IPLDSerializer**: Serializes objects to DAG-CBOR format

## Configuration

Plugin is configured via `nextflow.config`:

```groovy
plugins {
    id 'nf-blocks@0.1.0'
}

blocks {
    enabled = true
    storage.uri = 'blocks+file:///path/to/storage'  // or blocks+s3://bucket/path
    refs.uri = 'refs+file:///path/to/refs'          // or refs+s3://bucket/path
}

// Set outputDir to use blocks storage
outputDir = 'blocks+file:///path/to/storage'
```

## Storage Layout

```
{storage-root}/
├── .staging/                          ← Temp files during upload (cleaned up)
│   └── {uuid}.tmp
├── blobs/                             ← Content-addressed storage
│   ├── bafkr4i...                     ← Data blobs (by BLAKE3 CID)
│   ├── bafkr4i...                     ← More blobs
│   └── bafyr4i...                     ← Manifests (dag-cbor CID)
└── refs/                              ← Mutable references
    ├── workflows/{name}/latest        ← Points to latest run manifest CID
    ├── projects/{id}/latest
    └── runs/{run-id}                  ← Permanent run reference
```

## Important Implementation Notes

### Filesystem Provider Registration
- **Do NOT use META-INF/services** - Nextflow plugins use isolated classloaders
- **Must use** `FileHelper.getOrInstallProvider(ProviderClass)` in `BlocksPlugin.start()`
- Providers registered programmatically, not via Java ServiceLoader

### CID Format
- All blobs stored by CIDv1 format (`bafkr4i...` for raw data)
- Use `CID.fromHash(blake3Hash)` to convert hex hash to CID
- Never store raw hashes as filenames

### BLAKE3 Hash Calculation
- Use `b3sum` CLI via `ProcessBuilder` for streaming calculation
- **Important:** Use `b3sum --no-names` flag to get only the hash output
- Stream data simultaneously to file/S3 and hash process (zero memory overhead)

### Path Resolution
- `BlocksFileSystemProvider.parseBlocksUri()` extracts underlying scheme from `blocks+s3://` → `s3://`
- Base storage dir comes from URI path, not filesystem root
- Example: `blocks+file:///tmp/foo` → base dir `/tmp/foo`, not `/`

### S3 Multipart Upload
- Minimum part size: 5MB (AWS requirement)
- Staged upload: `.staging/{uuid}` → `blobs/{cid}` via server-side CopyObject
- CopyObject is atomic and free (no data transfer costs)
- Always AbortMultipartUpload on errors to avoid orphaned parts

## Dependencies

Key external dependencies from `build.gradle`:
- AWS SDK: `software.amazon.awssdk:s3`, `software.amazon.awssdk:sts`
- Jackson: CBOR serialization for IPLD (`jackson-dataformat-cbor`)
- Commons Codec: Base32 encoding for CIDs
- Nextflow Plugin SDK: `io.nextflow.nextflow-plugin:1.0.0-beta.10`

**External tools required:**
- `b3sum` CLI: BLAKE3 hash calculation (must be in PATH)

## Common Pitfalls

1. **Forgetting `make install`**: Changes won't take effect until plugin is reinstalled
2. **Wrong base storage dir**: Extracting path from URI requires special handling for `file://` scheme
3. **CID format confusion**: Always use `CID.fromHash()`, never store raw hex hashes
4. **Process cleanup**: Always close `b3sum` process streams and wait for exit
5. **S3 multipart lifecycle**: Must complete or abort uploads; staging keys need cleanup
6. **Atomic operations**: Use `Files.move()` with `ATOMIC_MOVE` for local, CopyObject for S3

## Related Documentation

- `examples/`: Test workflows for local development
