import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { SCHEMA_VERSION, SNAPSHOT_PATH } from '../src/config.js'

test('the page reads the snapshot of the schema the plugin writes', () => {
  const index = readFileSync(new URL('../../src/main/groovy/robsyme/cas/core/Index.groovy', import.meta.url), 'utf8')
  const declared = Number(/static final int SCHEMA_VERSION = (\d+)/.exec(index)[1])
  assert.equal(SCHEMA_VERSION, declared)
  assert.equal(SNAPSHOT_PATH, `index/v${declared}.sqlite`)
})
