"""DAG-JSON as the Gate reads it, and the blocks the plugin must build from a
request (block explorer spec sections 7.1, 8 and 9.1), rebuilt with the Gate's
own encoder so no Selection or Claim address is taken on trust (spec section
1.3, assertions 8 and 10; DESIGN.md section 0 rule 6)."""
import base64
import json

import cas

OCCURRENCE = "cas://"


def loads(text):
    return _convert(json.loads(text))


def _convert(value):
    if isinstance(value, dict):
        if "/" in value:
            if len(value) != 1:
                raise cas.GateError('a map with a "/" key and others: %r' % (value,))
            inner = value["/"]
            if isinstance(inner, str):
                return cas.Cid(inner)
            if isinstance(inner, dict) and list(inner) == ["bytes"] and isinstance(inner["bytes"], str):
                text = inner["bytes"]
                return base64.b64decode(text + "=" * (-len(text) % 4))
            raise cas.GateError('a "/" map that is neither a link nor bytes: %r' % (value,))
        return {k: _convert(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_convert(v) for v in value]
    return value


def _text_of_binary(binary):
    return "b" + cas._b32_encode(binary)


def expected_selection(request, asserted_by):
    """Members merged by address (via unioned), sorted by address string; via and derived_from sorted likewise."""
    members = {}
    for m in request["members"]:
        if isinstance(m, str):
            parts = m[len(OCCURRENCE):].split("/")
            key, nested, via = parts[1], False, {parts[0]}
        elif "selection" in m:
            key, nested, via = m["selection"].text, True, set()
        else:
            key, nested, via = m["item"]["address"].text, False, {v.text for v in m["item"].get("via", [])}
        seen = members.setdefault(key, [nested, set()])
        if seen[0] != nested:
            raise cas.GateError("%s is both an item and a Selection" % key)
        seen[1].update(via)
    out = []
    for key in sorted(members):
        nested, via = members[key]
        out.append({"selection": cas.Cid(key)} if nested
                   else {"item": {"address": cas.Cid(key), "via": [cas.Cid(v) for v in sorted(via)]}})
    derived = set()
    for d in request.get("derived_from") or []:
        derived.add(d.text if isinstance(d, cas.Cid) else _text_of_binary(d))
    return {"kind": "Selection", "schema": 1, "asserted_by": asserted_by, "members": out,
            "derived_from": [cas.Cid(t).binary() for t in sorted(derived)]}


def expected_claim(request, asserted_by):
    supersedes = sorted({s.text for s in request.get("supersedes") or []})
    return {"kind": "Claim", "schema": 1, "asserted_by": asserted_by, "subject": request["subject"],
            "verb": request["verb"], "attribute": request.get("attribute"), "value": request.get("value"),
            "supersedes": [cas.Cid(s) for s in supersedes], "timestamp": request["timestamp"]}


def address(block):
    return cas.cid_dagcbor(cas.encode(block))


def claim_state(claims):
    """Decision 5 of the milestone 2 plan: the same rules as ClaimState.groovy and claims.js."""
    superseded = {s for c in claims for s in (c.get("supersedes") or [])}
    current = sorted((c for c in claims if c["cid"] not in superseded), key=lambda c: c["cid"])
    names = [c for c in current if c.get("attribute") == "name"]
    deletion = [c for c in current if c.get("attribute") is None and c["verb"] in ("delete", "del")]
    state = "conflicted" if len(deletion) > 1 else "deleted" if len(deletion) == 1 and deletion[0]["verb"] == "delete" else "none"
    return {"current": [c["cid"] for c in current],
            "names": [str(c["value"]) for c in names if c["verb"] == "set"],
            "name_conflicted": len(names) > 1,
            "deletion": state}
