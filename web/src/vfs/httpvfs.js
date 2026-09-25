// A read-only SQLite VFS over synchronous byte sources (spec section 5.2).
// SQLite asks for pages one at a time; each read goes to the source
// registered under the file's name. Nothing is ever written, locked or synced.

export function installReadOnlyVfs(sqlite3, name) {
  const { capi, wasm } = sqlite3
  const sources = new Map()   // file name -> source
  const open = new Map()      // sqlite3_file pointer -> source
  const state = { lastError: null }
  const fail = (error, rc) => { state.lastError = error; return rc }

  const io = new capi.sqlite3_io_methods()
  io.$iVersion = 1
  sqlite3.vfs.installVfs({
    io: {
      struct: io,
      methods: {
        xClose(pFile) { open.delete(pFile); return 0 },
        xRead(pFile, pDest, n, offset64) {
          try {
            const got = open.get(pFile).read(Number(offset64), n)
            const dest = Number(pDest)
            const heap = wasm.heap8u()
            heap.set(got, dest)
            if (got.length < n) {
              heap.fill(0, dest + got.length, dest + n)
              return capi.SQLITE_IOERR_SHORT_READ
            }
            return 0
          } catch (e) {
            return fail(e, capi.SQLITE_IOERR_READ)
          }
        },
        xWrite: () => capi.SQLITE_READONLY,
        xTruncate: () => capi.SQLITE_READONLY,
        xSync: () => 0,
        xFileSize(pFile, pSize) { wasm.poke64(pSize, BigInt(open.get(pFile).size)); return 0 },
        xLock: () => 0,
        xUnlock: () => 0,
        xCheckReservedLock(pFile, pOut) { wasm.poke32(pOut, 0); return 0 },
        xFileControl: () => capi.SQLITE_NOTFOUND,
        xSectorSize: () => 4096,
        xDeviceCharacteristics: () => capi.SQLITE_IOCAP_IMMUTABLE,
      },
    },
  })

  const vfs = new capi.sqlite3_vfs()
  const fallback = new capi.sqlite3_vfs(capi.sqlite3_vfs_find(null))
  vfs.$iVersion = 1
  vfs.$szOsFile = capi.sqlite3_file.structInfo.sizeof
  vfs.$mxPathname = 1024
  vfs.$zName = wasm.allocCString(name)
  vfs.$xRandomness = fallback.$xRandomness
  vfs.$xSleep = fallback.$xSleep
  vfs.$xCurrentTime = fallback.$xCurrentTime
  vfs.$xCurrentTimeInt64 = fallback.$xCurrentTimeInt64
  fallback.dispose()
  sqlite3.vfs.installVfs({
    vfs: {
      struct: vfs,
      methods: {
        xOpen(pVfs, zName, pFile, flags, pOutFlags) {
          const key = zName ? wasm.cstrToJs(zName) : ''
          const source = sources.get(key)
          if (!source) return fail(new Error(`no source registered as '${key}'`), capi.SQLITE_CANTOPEN)
          open.set(pFile, source)
          const file = new capi.sqlite3_file(pFile)
          file.$pMethods = io.pointer
          file.dispose()
          wasm.poke32(pOutFlags, capi.SQLITE_OPEN_READONLY)
          return 0
        },
        xDelete: () => capi.SQLITE_IOERR_DELETE,
        xAccess(pVfs, zName, flags, pOut) { wasm.poke32(pOut, 0); return 0 },
        xFullPathname(pVfs, zName, nOut, pOut) {
          return wasm.cstrncpy(pOut, zName, nOut) < nOut ? 0 : capi.SQLITE_CANTOPEN
        },
        xGetLastError: () => 0,
      },
    },
  })

  return {
    register(key, source) { sources.set(key, source) },
    unregister(key) { sources.delete(key) },
    get lastError() { return state.lastError },
  }
}
