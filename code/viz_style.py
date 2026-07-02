# ==========================================================================
# viz_style.py — shared minimalist visual style for the Diridon memo figures.
#
# Few colors, interesting hues: warm neutrals carry the base, a single PLUM
# ramp shades displacement vulnerability, and a single CORAL accent is reserved
# for the one main point — soft sites (where new homes can actually go).
# ==========================================================================

import matplotlib.pyplot as plt

# --- palette ---------------------------------------------------------------
INK = "#23201C"          # near-black warm charcoal (text, lines)
PAPER = "#FBF9F4"        # warm off-white background
MUTE = "#8C857A"         # muted warm grey (secondary text)

NEUTRAL = "#D8D2C6"      # base parcels / "already built" target parcels
NEUTRAL_D = "#B3AB9C"    # slightly darker neutral

CORAL = "#E25A33"        # THE accent — soft sites, the headline number
CORAL_SOFT = "#F0B59F"   # light coral for fills/secondary emphasis

# plum sequential ramp for vulnerability score (0 -> 5)
PLUM = ["#EFE9EC", "#D8C7D5", "#B79DBB", "#8F6E9C", "#5E4B6B", "#3E3049"]
PLUM_DEEP = "#5E4B6B"

CAPACITY_TIER = {                      # capacity tiers (kept low-saturation)
    "Downtown (DC)": "#6B7A8F",        # slate blue-grey
    "Mixed-use (UV/TR/UR/MUC/MUN)": NEUTRAL_D,
}


def apply_style():
    """Apply the shared minimalist rcParams."""
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.sans-serif": ["Helvetica Neue", "Avenir Next", "Helvetica", "Arial"],
        "figure.facecolor": PAPER,
        "axes.facecolor": PAPER,
        "savefig.facecolor": PAPER,
        "text.color": INK,
        "axes.edgecolor": INK,
        "axes.labelcolor": INK,
        "xtick.color": INK,
        "ytick.color": INK,
        "axes.linewidth": 0.8,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.titlesize": 13,
        "axes.titleweight": "bold",
        "axes.titlelocation": "left",
        "axes.titlepad": 10,
        "font.size": 10.5,
        "figure.dpi": 110,
        "savefig.dpi": 220,
        "savefig.bbox": "tight",
    })


def save(fig, name, output_dir):
    """Save a figure as both PNG (web) and PDF (print)."""
    from pathlib import Path
    d = Path(output_dir)
    d.mkdir(parents=True, exist_ok=True)
    fig.savefig(d / f"{name}.png")
    fig.savefig(d / f"{name}.pdf")
    plt.close(fig)
    return d / f"{name}.png"
