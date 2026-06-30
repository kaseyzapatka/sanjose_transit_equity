#!/bin/bash
# Post-render hook: ensure the Typst PDF of the memo is built into docs/.
#
# The PDF filename is set via `output-file:` in index.qmd's typst format, so it
# lands at docs/diridon_capacity_equity_memo.pdf — matching both the navbar
# "Download PDF" link and Quarto's auto-generated "Other Formats" link. No
# renaming (renaming previously broke the "Other Formats" link).
#
# Guard against recursion: the render below re-triggers this hook.
if [ -n "$DIRIDON_RENDERING_PDF" ]; then
  exit 0
fi
export DIRIDON_RENDERING_PDF=1
set -e

echo "Rendering memo PDF (Typst)..."
quarto render index.qmd --to typst
echo "  docs/diridon_capacity_equity_memo.pdf done"
