# nf-blocks: hash this task's declared outputs on the node (DESIGN.md §11, spec §14).
# Installed as a process.afterScript default, ahead of the user's. Reads the
# "### outputs:" block of .command.run, expands each pattern, and writes
# .command.cas in sha256sum format, paths relative to the task directory.
# It never fails the task: every error leaves .command.cas absent or partial,
# and the head node then addresses the file itself.
nf_blocks_cas() {
  local dir="${NXF_CHDIR:-$PWD}"
  [ -f "$dir/.command.run" ] || return 0
  local -a sum
  if command -v sha256sum >/dev/null 2>&1; then sum=(sha256sum)
  elif command -v shasum >/dev/null 2>&1; then sum=(shasum -a 256)
  else return 0; fi
  (
    cd "$dir" || exit 0
    shopt -s nullglob
    shopt -s globstar 2>/dev/null || true
    : > .command.cas.tmp || exit 0
    sed -n "s/^### - '\(.*\)'\$/\1/p" .command.run | while IFS= read -r pattern; do
      local IFS=
      for f in $pattern; do
        [ -L "$f" ] && continue
        if [ -d "$f" ]; then find "$f" -type f -exec "${sum[@]}" {} + 2>/dev/null
        elif [ -f "$f" ]; then "${sum[@]}" "$f" 2>/dev/null
        fi
      done
    done >> .command.cas.tmp
    mv -f .command.cas.tmp .command.cas
  ) || true
  rm -f "$dir/.command.cas.tmp" 2>/dev/null || true
  return 0
}
nf_blocks_cas || true
