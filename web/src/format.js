// web/src/format.js
// How the page writes bytes and numbers (the kit's rules, mockups/src/kit.js):
// bytes in SI units to three significant figures, trailing zeros dropped; integers, and floats stored
// with an integral value ("49791.0"), with thousands separators; other floats
// to three significant figures. The locale is fixed so every reader, and every
// test, sees the same text.
const LOCALE = 'en-US'
const UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']

// Decimal places for a value in a unit: three significant figures.
const places = (v) => (v >= 100 ? 0 : v >= 10 ? 1 : 2)

/**
 * `1.50` as `1.5`, `1.00` as `1`: trailing zeros after a decimal point dropped, and the point with them when nothing
 * is left. Only plain decimal text (`/^-?\d+\.\d+$/`) is trimmed; `1.2e+30` and `120` come back unchanged.
 */
export const trimZeros = (text) => {
  const t = String(text)
  return /^-?\d+\.\d+$/.test(t) ? t.replace(/\.?0+$/, '') : t
}

/** A ratio such as dedup: `3.1×`, `1×`, `3.08×`, two decimal places at most, trailing zeros dropped. */
export const ratioText = (n) => `${trimZeros(Number(n).toFixed(2))}×`

/**
 * `5.29 MB`, `21.1 MB`, `142 MB`, `1.5 MB`, `0 B`; a dash when the size is
 * unknown. Three significant figures, trailing zeros dropped. The value is
 * rounded before its unit is final, so 999,999 B reads `1 MB` and 9,999 B
 * `10 KB` (kit.js picks the unit first and gives `1000 KB`).
 */
export function bytes(n) {
  if (n === null || n === undefined) return '—'
  let v = Number(n)
  let i = 0
  while (v >= 1000 && i < UNITS.length - 1) { v /= 1000; i++ }
  if (i === 0) return `${v} B`
  let p = places(v)
  const rounded = Number(v.toFixed(p))
  if (rounded >= 1000 && i < UNITS.length - 1) { v /= 1000; i++; p = 2 } else p = places(rounded)
  return `${trimZeros(v.toFixed(p))} ${UNITS[i]}`
}

/**
 * A size as every page writes it (`{ bytes, partial }` from the size walks):
 * `5.29 MB`, `at least 5.29 MB` when some blocks could not be read, a dash
 * when the size is unknown. A floor of zero says nothing, so it reads `0 B`.
 */
export function sizeText(size) {
  if (size?.bytes === null || size?.bytes === undefined) return '—'
  // `upTo`: an upper bound (Explorer.pipelineSize with newer runs added), which wins over `partial`'s floor.
  return `${size.upTo ? 'up to ' : size.partial && Number(size.bytes) !== 0 ? 'at least ' : ''}${bytes(size.bytes)}`
}

const INTEGRAL = /^\s*(-?\d+)(?:\.0*)?\s*$/

/** A number as the page shows it: `99,566`, `89.6`, `89`, `0.5`, `0.000123`. Text that is no number comes back unchanged. */
export function formatNumber(value) {
  const text = String(value)
  const integral = text.match(INTEGRAL)
  // BigInt keeps integers past 2^53 exact.
  if (integral) return BigInt(integral[1]).toLocaleString(LOCALE)
  const v = Number(value)
  if (typeof value === 'string' && text.trim() === '') return text
  if (!Number.isFinite(v)) return text
  if (Number.isInteger(v)) return v.toLocaleString(LOCALE)
  // At most three significant figures, trailing zeros dropped: 88.99 is `89`, 0.5 is `0.5`.
  return v.toLocaleString(LOCALE, { maximumSignificantDigits: 3 })
}

const truth = (value) => value === true || value === 'true' || value === 1 || value === '1'

/**
 * A typed value for display. `field`, a promoted field, may carry
 * `format: 'int' | 'percent'` and boolean `labels: { true, false }`.
 */
export function formatValue(type, value, field = null) {
  if (type === 'null' || value === null || value === undefined) return 'null'
  if (type === 'bool') {
    const key = String(truth(value))
    return field?.labels?.[key] ?? key
  }
  if (type === 'int' || type === 'float') {
    const v = field?.format === 'int' && Number.isFinite(Number(value)) ? Math.round(Number(value)) : value
    const text = formatNumber(v)
    return field?.format === 'percent' ? `${text}%` : text
  }
  return String(value)
}

/** `1 Selection`, `2,000 items`: a count, written as formatNumber does, and its noun. */
export const plural = (n, one, many = `${one}s`) => `${formatNumber(n)} ${n === 1 ? one : many}`
