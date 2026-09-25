// DAG-CBOR decoded with its floats kept apart from its integers. JavaScript
// has one number type, so 30.0 and 30 decode alike; the index types them
// 'float' and 'int' (MetadataView.scalar), and query 3 over a stale run must
// type them the same way.
import { Token, Tokenizer, Type, decode } from 'cborg'
import * as dagCbor from '@ipld/dag-cbor'

export class Float {
  constructor(value) { this.value = value }
}

export function typedDecode(bytes) {
  const inner = new Tokenizer(bytes, dagCbor.decodeOptions)
  const tokenizer = {
    done: () => inner.done(),
    pos: () => inner.pos(),
    next() {
      const token = inner.next()
      return Type.equals(token.type, Type.float) ? new Token(Type.float, new Float(token.value), token.encodedLength) : token
    },
  }
  return decode(bytes, { ...dagCbor.decodeOptions, tokenizer })
}
