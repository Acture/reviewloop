#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <output-path>" >&2
  exit 1
fi

output_path="$1"
crate_name="${CRATE_NAME:-$(sed -nE 's/^name[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' Cargo.toml | head -n1)}"
version="${VERSION:?VERSION is required}"
tag_name="${TAG_NAME:-v${version}}"
source_sha256="${SOURCE_SHA256:?SOURCE_SHA256 is required}"

formula_class=$(echo "${crate_name}" | awk -F '[-_]' '{for (i=1; i<=NF; i++) printf toupper(substr($i,1,1)) substr($i,2)}')
description="$(sed -nE 's/^description[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' Cargo.toml | head -n1)"
homepage="$(sed -nE 's/^homepage[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' Cargo.toml | head -n1)"
license_name="$(sed -nE 's/^license[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' Cargo.toml | head -n1)"
repository_url="$(sed -nE 's/^repository[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' Cargo.toml | head -n1)"
repo_slug="${GITHUB_REPOSITORY:-${repository_url#https://github.com/}}"
source_url="https://github.com/${repo_slug}/archive/refs/tags/${tag_name}.tar.gz"

mkdir -p "$(dirname "${output_path}")"

cat > "${output_path}" <<EOF
class ${formula_class} < Formula
  desc "${description}"
  homepage "${homepage}"
  url "${source_url}"
  sha256 "${source_sha256}"
  license "${license_name}"

  livecheck do
    url :stable
    regex(/^v?(\\d+(?:\\.\\d+)+)$/i)
  end

  depends_on "rust" => :build

  on_linux do
    depends_on "openssl@3"
    depends_on "pkgconf" => :build
  end

  def install
    ENV["OPENSSL_DIR"] = Formula["openssl@3"].opt_prefix if OS.linux?
    ENV["OPENSSL_NO_VENDOR"] = "1" if OS.linux?

    system "cargo", "install", "--locked", *std_cargo_args(path: ".")
  end

  test do
    ENV["HOME"] = testpath
    ENV["XDG_CONFIG_HOME"] = testpath/".config"
    ENV["REVIEWLOOP_STATE_DIR"] = testpath/".review_loop"

    (testpath/"paper.pdf").write("%PDF-1.4\\n")
    (testpath/"reviewloop-test.toml").write <<~TOML
      [logging]
      output = "file"
    TOML

    system bin/"${crate_name}", "--config", testpath/"reviewloop-test.toml",
      "paper", "add",
      "--paper-id", "main",
      "--pdf-path", testpath/"paper.pdf",
      "--backend", "stanford",
      "--no-submit-prompt"

    config_path = testpath/".config/reviewloop/reviewloop.toml"
    assert_path_exists config_path
    assert_includes config_path.read, "[providers.stanford]"

    output = shell_output("#{bin}/${crate_name} --config #{testpath/"reviewloop-test.toml"} status --json")
    assert_equal "[]\\n", output
    assert_path_exists testpath/".review_loop/reviewloop.db"
  end
end
EOF
