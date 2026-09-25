// Builds single-file pages into dist/: the app, the Worker source and
// sqlite3.wasm are inlined so a member can carry the page as one index.html
// (DESIGN.md §15). The IPLD Schema is regenerated from DESIGN.md first.
import * as esbuild from 'esbuild'
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { extractSchema } from './schema-gen.mjs'

const here = new URL('.', import.meta.url)
const path = (p) => new URL(p, here).pathname

mkdirSync(path('src/generated'), { recursive: true })
writeFileSync(path('src/generated/schema.json'), JSON.stringify(extractSchema(readFileSync(path('../DESIGN.md'), 'utf8'))))

async function bundle(entry, define = {}) {
  const result = await esbuild.build({
    entryPoints: [path(entry)], bundle: true, format: 'iife', write: false, minify: true,
    target: 'es2022', define, logLevel: 'warning', legalComments: 'none',
  })
  return result.outputFiles[0].text
}

async function page(entry, template, out, define) {
  const js = await bundle(entry, define)
  const html = readFileSync(path(template), 'utf8')
    .replace('<!--APP-->', () => `<script>${js.replace(/<\/script/gi, '<\\/script')}</script>`)
  mkdirSync(path('dist'), { recursive: true })
  writeFileSync(path(`dist/${out}`), html)
  console.log(`dist/${out} ${html.length} bytes`)
}

const worker = await bundle('src/worker.js', { 'import.meta.url': JSON.stringify('https://nf-blocks.invalid/worker.js') })
const wasm = readFileSync(path('node_modules/@sqlite.org/sqlite-wasm/dist/sqlite3.wasm'))
const inlined = { __WORKER_SOURCE__: JSON.stringify(worker), __WASM_BASE64__: JSON.stringify(wasm.toString('base64')) }

await page('bench/bench.js', 'bench/bench.html', 'bench.html', inlined)
await page('src/app.js', 'src/index.html', 'index.html', inlined)
