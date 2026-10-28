// What a server said about a failed read, when it said it in plain text:
// nf-blocks:explore explains a failure that way (a credentials one says what
// to do). Another server's HTML or XML error page is not shown.

/** The plain-text body of a failed `res`, trimmed; '' when it has none. */
export async function failureText(res) {
  if (!(res.headers?.get('Content-Type') ?? '').startsWith('text/plain')) return ''
  try {
    return (await res.text()).trim()
  } catch {
    return ''
  }
}
