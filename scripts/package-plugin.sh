#!/usr/bin/env bash
# Copy integrations/reviewloop into a package directory.
# The skill files are copied from the one source tree; this script does not
# keep a second skill body. --check packs to a temp directory and fails unless
# the packaged skill matches that source.

set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
src="${repo_root}/integrations/reviewloop"
docs="${repo_root}/docs/plugins.md"

fail() {
  printf 'package-plugin: %s\n' "$1" >&2
  exit 1
}

pack() {
  local out=$1
  local src_real

  [ -d "${src}/skills/reviewloop" ] || fail "missing skill source ${src}/skills/reviewloop"
  [ -f "${docs}" ] || fail "missing ${docs}"
  if find "${src}" -type l -print -quit | grep -q .; then
    fail "plugin source contains a symlink; keep the skill as real files under integrations/reviewloop/skills"
  fi

  mkdir -p -- "$(dirname -- "${out}")"
  src_real="$(cd -- "${src}" && pwd)"
  out="$(cd -- "$(dirname -- "${out}")" && pwd)/$(basename -- "${out}")"
  case "${out}" in
    "${src_real}" | "${src_real}"/*)
      fail "refusing to write the package inside the plugin source"
      ;;
    / | "${repo_root}")
      fail "refusing to replace ${out}"
      ;;
  esac

  rm -rf -- "${out}"
  mkdir -p -- "${out}"
  cp -R "${src}/." "${out}/"
  cp -- "${docs}" "${out}/README.md"
  chmod +x "${out}/bin/reviewloop-mcp"

  local packaged="${out}/skills/reviewloop"
  diff -rq "${src}/skills/reviewloop" "${packaged}"
  diff -q "${docs}" "${out}/README.md"
  local skill_count
  skill_count="$(find "${out}" -name SKILL.md -print | wc -l | tr -d ' ')"
  [ "${skill_count}" = 1 ] || fail "expected one SKILL.md in the package, found ${skill_count}"
  if find "${out}" -type l -print | grep -q .; then
    fail "package contains a symlink"
  fi
  if grep -n '\.\.' \
    "${out}/plugin.json" \
    "${out}/mcp.json" \
    "${out}/.claude-plugin/plugin.json" \
    "${out}/.codex-plugin/plugin.json" \
    "${out}/mcp/claude.json" \
    "${out}/mcp/codex.json" \
    >/dev/null
  then
    fail "a manifest path contains .."
  fi
}

if [ "${1:-}" = "--check" ]; then
  tmp="$(mktemp -d)"
  trap 'rm -rf -- "${tmp}"' EXIT
  pack "${tmp}/reviewloop-plugin"
  exit 0
fi

out="${1:-${repo_root}/dist/reviewloop-plugin}"
pack "${out}"
printf 'packaged %s\n' "${out}"
