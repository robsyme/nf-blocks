// Bundles mockup.js with the explorer's modules into one page, then screenshots it.
//   node docs/superpowers/specs/2026-10-28-lineage-provenance-tree/build.mjs [out-dir]
// Needs web/node_modules (npm ci in web) and gate/browser/node_modules (Playwright).
// Writes mockup.html and mockup.png (a 1280px window) to out-dir, by default this directory.
import { writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const repo = join(here, '../../../..')
const out = process.argv[2] ?? here
const esbuild = await import(pathToFileURL(join(repo, 'web/node_modules/esbuild/lib/main.js')))
const { chromium } = await import(pathToFileURL(join(repo, 'gate/browser/node_modules/playwright/index.mjs')))

const r = await esbuild.build({ entryPoints: [join(here, 'mockup.js')], bundle: true, write: false, format: 'iife', logLevel: 'warning' })
const js = r.outputFiles[0].text.replaceAll('</script', '<\\/script')
const html = join(out, 'mockup.html')
writeFileSync(html, `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Lineage mockup</title></head><body><script>${js}</script></body></html>`)

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1280, height: 900 } })
const errors = []
page.on('pageerror', e => errors.push(String(e)))
await page.goto(pathToFileURL(html).href)
await page.screenshot({ path: join(out, 'mockup.png'), fullPage: true })
await browser.close()
if (errors.length) { console.error(errors.join('\n')); process.exit(1) }
console.log(`wrote ${html} and mockup.png`)
