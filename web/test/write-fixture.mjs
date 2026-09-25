// web/test/write-fixture.mjs
// Writes the Task 10 fixture as a member directory a static server can serve,
// with the built page beside the snapshot, and prints the expected answers.
//   node web/test/write-fixture.mjs <dir>
import { copyFileSync, mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { buildMember } from './fixture.mjs'

const dir = process.argv[2]
const member = await buildMember()
for (const [cid, bytes] of member.blocks) {
  mkdirSync(join(dir, 'blocks', cid.slice(-2)), { recursive: true })
  writeFileSync(join(dir, 'blocks', cid.slice(-2), cid), bytes)
}
mkdirSync(join(dir, 'log'), { recursive: true })
for (const name of member.log) writeFileSync(join(dir, 'log', name), '')
mkdirSync(join(dir, 'index'), { recursive: true })
writeFileSync(join(dir, 'index', 'v2.sqlite'), member.snapshot)
copyFileSync(new URL('../dist/index.html', import.meta.url), join(dir, 'index.html'))
console.log(JSON.stringify({ runs: member.runs, item: member.item, content: member.content }))
