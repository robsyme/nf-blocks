// web/test/format.test.mjs
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { bytes, formatNumber, formatValue, ratioText, sizeText, trimZeros } from '../src/format.js'

test('bytes: SI units, three significant figures, a dash for nothing', () => {
  assert.equal(bytes(0), '0 B')
  assert.equal(bytes(999), '999 B')
  assert.equal(bytes(417000), '417 KB')
  assert.equal(bytes(5290000), '5.29 MB')
  assert.equal(bytes(21119476), '21.1 MB')
  assert.equal(bytes(142134100), '142 MB')
  assert.equal(bytes(1135852), '1.14 MB')
  assert.equal(bytes(1820000000000), '1.82 TB')
  assert.equal(bytes(null), '—')
  assert.equal(bytes(undefined), '—')
})

test('bytes: rounds first, then picks the unit, so a figure never reads 1000 or 10.00', () => {
  assert.equal(bytes(999_999), '1 MB')
  assert.equal(bytes(999_499), '999 KB')
  assert.equal(bytes(9_999), '10 KB')
  assert.equal(bytes(99_960), '100 KB')
  assert.equal(bytes(9_994), '9.99 KB')
  assert.equal(bytes(1000), '1 KB')
})

test('trimZeros drops trailing zeros after a decimal point, and the point when nothing is left', () => {
  assert.equal(trimZeros('1.00'), '1')
  assert.equal(trimZeros('21.0'), '21')
  assert.equal(trimZeros('5.29'), '5.29')
  assert.equal(trimZeros('1.50'), '1.5')
  assert.equal(trimZeros('120'), '120')
  assert.equal(trimZeros('100'), '100', 'zeros before any point stay')
  assert.equal(trimZeros('-2.50'), '-2.5')
  assert.equal(trimZeros('1.2e+30'), '1.2e+30', 'an exponent is no decimal fraction')
  assert.equal(trimZeros('1.0e+30'), '1.0e+30')
  assert.equal(trimZeros('1,000.0'), '1,000.0', 'only plain decimal text is trimmed')
})

test('bytes: three significant figures, trailing zeros dropped', () => {
  assert.equal(bytes(1_000_000), '1 MB')
  assert.equal(bytes(21_000_000), '21 MB')
  assert.equal(bytes(9_000_000_000), '9 GB')
  assert.equal(bytes(1_500_000), '1.5 MB')
  assert.equal(bytes(5_290_000), '5.29 MB')
  assert.equal(bytes(142_000_000), '142 MB')
})

test('sizeText: one wording for a size, partial or unknown', () => {
  assert.equal(sizeText({ bytes: 5_290_000, partial: false }), '5.29 MB')
  assert.equal(sizeText({ bytes: 5_290_000, partial: true }), 'at least 5.29 MB')
  // An upper bound (a pipeline's indexed figure plus newer runs that may share files with it) reads "up to", even when also partial.
  assert.equal(sizeText({ bytes: 5_290_000, partial: false, upTo: true }), 'up to 5.29 MB')
  assert.equal(sizeText({ bytes: 5_290_000, partial: true, upTo: true }), 'up to 5.29 MB')
  // Fix round 3: a floor of zero says nothing, so a zero reads plainly ("Releasing 0 B", never "at least 0 B").
  assert.equal(sizeText({ bytes: 0, partial: true }), '0 B')
  assert.equal(sizeText({ bytes: null }), '—')
  assert.equal(sizeText(null), '—')
  assert.equal(sizeText(undefined), '—')
})

test('formatNumber: integers and integral floats get separators, other floats at most three significant figures', () => {
  assert.equal(formatNumber(99566), '99,566')
  assert.equal(formatNumber('49791.0'), '49,791')
  assert.equal(formatNumber('12345678901234567890'), '12,345,678,901,234,567,890')
  assert.equal(formatNumber(-1234), '-1,234')
  assert.equal(formatNumber(0.5), '0.5', 'trailing zeros are dropped')
  assert.equal(formatNumber(88.98999786376953), '89')
  assert.equal(formatNumber(88.99), '89')
  assert.equal(formatNumber(89.04), '89')
  assert.equal(formatNumber(95.84), '95.8')
  assert.equal(formatNumber(95.85), '95.9', 'Intl rounds a half away from zero')
  assert.equal(formatNumber(89.0), '89', 'stored integral, it drops .0')
  assert.equal(formatNumber('89.0'), '89')
  assert.equal(formatNumber('89.62899137102112'), '89.6')
  assert.equal(formatNumber('not a number'), 'not a number')
})

test('formatValue: numbers, percent fields, boolean labels, strings and null', () => {
  assert.equal(formatValue('float', '99566.0'), '99,566')
  assert.equal(formatValue('float', '49791.0'), '49,791')
  assert.equal(formatValue('int', '1234567'), '1,234,567')
  assert.equal(formatValue('float', '89.62899137102112'), '89.6')
  assert.equal(formatValue('float', '89.62899137102112', { format: 'percent' }), '89.6%')
  assert.equal(formatValue('float', '88.98999786376953', { format: 'percent' }), '89%', 'trailing zeros are dropped')
  assert.equal(formatValue('float', '0.000123456'), '0.000123')
  assert.equal(formatValue('float', '1234.5678', { format: 'int' }), '1,235')
  assert.equal(formatValue('bool', 'true', { labels: { true: 'single', false: 'paired' } }), 'single')
  assert.equal(formatValue('bool', false, { labels: { true: 'single', false: 'paired' } }), 'paired')
  assert.equal(formatValue('bool', 'false'), 'false')
  assert.equal(formatValue('string', '99566'), '99566')
  assert.equal(formatValue('null', null), 'null')
})

test('ratioText: two decimal places at most, trailing zeros dropped', () => {
  assert.equal(ratioText(3.1), '3.1×')
  assert.equal(ratioText(1), '1×')
  assert.equal(ratioText(3.08), '3.08×')
  assert.equal(ratioText(3.0999), '3.1×')
  assert.equal(ratioText(5.6 / 1.82), '3.08×')
  assert.equal(ratioText(1.004), '1×')
})
