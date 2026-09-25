// The Worker source and sqlite3.wasm are inlined by build.mjs, so the page is
// one index.html a member can carry beside its snapshot (DESIGN.md §15).
/* global __WORKER_SOURCE__, __WASM_BASE64__ */

export function createInlineWorker() {
  const url = URL.createObjectURL(new Blob([__WORKER_SOURCE__], { type: 'text/javascript' }))
  return new Worker(url)
}

let wasm
export function inlineWasm() {
  if (!wasm) {
    const text = atob(__WASM_BASE64__)
    wasm = new Uint8Array(text.length)
    for (let i = 0; i < text.length; i++) wasm[i] = text.charCodeAt(i)
  }
  return wasm
}
