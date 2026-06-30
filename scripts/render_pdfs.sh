#!/bin/bash
# Post-render hook: produce a PDF of the memo (via Quarto's bundled Typst —
# no LaTeX needed) and place it in docs/ so the navbar "Download PDF" link
# resolves on the published site.
#
# Guard against recursion: the typst render below re-triggers this hook.
if [ -n "$DIRIDON_RENDERING_PDF" ]; then
  exit 0
fi
export DIRIDON_RENDERING_PDF=1
set -e

PDF_NAME="diridon_capacity_equity_memo.pdf"

echo "Rendering memo PDF (Typst)..."
quarto render index.qmd --to typst

# The typst output lands at docs/index.pdf (project output-dir) or ./index.pdf
# depending on Quarto version; normalize to docs/<PDF_NAME>.
mkdir -p docs
if [ -f docs/index.pdf ]; then
  mv -f docs/index.pdf "docs/${PDF_NAME}"
elif [ -f index.pdf ]; then
  mv -f index.pdf "docs/${PDF_NAME}"
fi

if [ -f "docs/${PDF_NAME}" ]; then
  echo "  docs/${PDF_NAME} done"
else
  echo "  WARNING: PDF not found after render" >&2
fi
