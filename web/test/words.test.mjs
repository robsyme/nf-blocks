// web/test/words.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { anomalyLines, healthSummary, healthText, humanBytes, leafReasonText, statusText, statusTone, whenText } from '../src/words.js'

const RNASEQ = { unresolvable: 0, unaddressed: 0, declined: 439, never_published: 5, unjoined: 5 }

test('anomaly lines: non-zero only, concerns first, in the spec\'s words', () => {
  assert.deepEqual(anomalyLines(RNASEQ), [
    { key: 'unjoined', count: 5, concern: true, text: '5 published files that are in no output' },
    { key: 'declined', count: 439, concern: false, text: '439 empty optional slots (the pipeline passed no file)' },
    { key: 'never_published', count: 5, concern: false, text: '5 referenced files not stored here, such as input files' },
  ])
  assert.equal(anomalyLines({ unjoined: 1 })[0].text, '1 published file that is in no output')
  assert.deepEqual(anomalyLines({}), [])
  assert.deepEqual(anomalyLines(undefined), [])
})

test('an older block without unjoined reads it as 0', () => {
  assert.deepEqual(anomalyLines({ unresolvable: 0, unaddressed: 0, declined: 0, never_published: 0 }), [])
})

test('health: worth a look when any concern is non-zero', () => {
  assert.equal(healthSummary(RNASEQ), 'worth a look')
  assert.equal(healthSummary({ declined: 3, never_published: 2 }), 'nothing to check')
  assert.equal(healthSummary({ unresolvable: 1 }), 'worth a look')
  assert.equal(healthSummary({ unaddressed: 1 }), 'worth a look')
  assert.equal(healthText({ declined: 3 }), 'nothing to check')
  assert.equal(healthText(RNASEQ), '5 published files that are in no output')
})

test('status in words, and its tone', () => {
  assert.equal(statusText({ status: 'succeeded', possibly_incomplete: false }), 'Succeeded')
  assert.equal(statusText({ status: 'failed', possibly_incomplete: true }), 'Failed, may be incomplete')
  assert.equal(statusText({ status: 'failed', possibly_incomplete: false }), 'Failed')
  assert.equal(statusText({ status: 'succeeded', possibly_incomplete: 1 }), 'Succeeded, may be incomplete')
  assert.equal(statusTone({ status: 'succeeded', possibly_incomplete: 0 }), 'good')
  assert.equal(statusTone({ status: 'succeeded', possibly_incomplete: 1 }), 'bad')
  assert.equal(statusTone({ status: 'failed', possibly_incomplete: true }), 'bad')
})

test('sizes in decimal units; a BigInt, null and empty are handled', () => {
  assert.equal(humanBytes(0), '0 B')
  assert.equal(humanBytes(999), '999 B')
  assert.equal(humanBytes(1200), '1.2 KB')
  assert.equal(humanBytes(412_000_000), '412 MB')
  assert.equal(humanBytes(3n * 1000n ** 3n), '3.0 GB')
  assert.equal(humanBytes(null), '')
  assert.equal(humanBytes(''), '')
})

test('a time reads in the viewer\'s locale; text that is not a time is shown as it is', () => {
  assert.notEqual(whenText('2026-09-30T21:10:38.345Z'), '2026-09-30T21:10:38.345Z')
  assert.equal(whenText('not a time'), 'not a time')
  assert.equal(whenText(null), '')
})

test('a leaf with no address says why, in words', () => {
  assert.equal(leafReasonText('declined'), 'no file (an optional output was empty)')
  assert.equal(leafReasonText('never_published'), 'not stored here (such as an input file)')
  assert.equal(leafReasonText('unresolvable'), 'a link whose target could not be found')
  assert.equal(leafReasonText('unaddressed'), 'no stored content')
  assert.equal(leafReasonText('something new'), 'something new')
})
