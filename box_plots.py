import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker
import numpy as np

try:
    import seaborn as sns
    sns.set_style("whitegrid")
except ImportError:
    pass

tables_dir = "tables"
output_dir = os.path.join(tables_dir, "figures")
os.makedirs(output_dir, exist_ok=True)

pattern = os.path.join(tables_dir, "*_descriptive_statistics.csv")
files = sorted(glob.glob(pattern))

if not files:
    raise FileNotFoundError(f"No *_descriptive_statistics.csv files found in {tables_dir}/")

print(f"Found {len(files)} company tables:")

# Mapping from raw filename prefix to display name with country prefix
company_rename = {
    "Alibaba": "cn_Alibaba",
    "Anthropic": "us_Anthropic",
    "DeepMind": "us_DeepMind",
    "DeepSeek": "cn_DeepSeek",
    "Moonshot": "cn_Moonshot",
    "OpenAI": "us_OpenAI",
}

company_data = {}
for f in files:
    basename = os.path.basename(f)
    raw_name = basename.replace("_descriptive_statistics.csv", "")
    display_name = company_rename.get(raw_name, raw_name)
    company_data[display_name] = pd.read_csv(f)
    print(f"  {display_name}: {basename}")

# Sort alphabetically so cn_ companies group together, then us_
companies = sorted(company_data.keys())

# Color mapping: consistent colors by display name
company_colors = {
    "cn_Alibaba":   ("#E06B54", "#B22222"),  # red
    "cn_DeepSeek":  ("#6BC76B", "#2D8E2D"),  # green
    "cn_Moonshot":  ("#E0A555", "#A87222"),  # gold
    "us_Anthropic": ("#F4A942", "#E07B15"),  # orange
    "us_DeepMind":  ("#5B9BD5", "#2E75B6"),  # blue
    "us_OpenAI":    ("#B07DDB", "#7A4DA8"),  # purple
}

# Fallback palette for any unlisted company
fallback_pairs = [
    ("#55C4C4", "#228E8E"),
    ("#DB7DBF", "#A84D8E"),
    ("#8FBC8F", "#5F8F5F"),
    ("#CD853F", "#8B5A2B"),
]

numeric_vars = [
    "works_count",
    "cited_by_count",
    "2yr_mean_citedness",
    "h_index",
    "i10_index",
    "unique_affiliation_count",
    "unique_country_count",
]

boolean_vars = [
    "all_china",
    "some_china",
    "some_usa",
    "all_usa",
    "affiliation_data",
]


def get_colors(company_name, fallback_idx):
    if company_name in company_colors:
        return company_colors[company_name]
    return fallback_pairs[fallback_idx % len(fallback_pairs)]


def draw_horizontal_box(ax, y_pos, stats, color_light, color_dark, height=0.45):
    q1 = stats["25th_percentile"]
    median = stats["median_50th"]
    q3 = stats["75th_percentile"]
    vmin = stats["min"]
    vmax = stats["max"]
    mean = stats["mean"]

    floor = 0.5
    q1 = max(q1, floor)
    median = max(median, floor)
    q3 = max(q3, floor)
    vmin = max(vmin, floor)
    vmax = max(vmax, floor)
    mean = max(mean, floor)

    left_box = mpatches.Rectangle(
        (q1, y_pos - height / 2), median - q1, height,
        facecolor=color_light, edgecolor="black", linewidth=0.8,
    )
    ax.add_patch(left_box)

    right_box = mpatches.Rectangle(
        (median, y_pos - height / 2), q3 - median, height,
        facecolor=color_dark, edgecolor="black", linewidth=0.8,
    )
    ax.add_patch(right_box)

    ax.plot([median, median], [y_pos - height / 2, y_pos + height / 2],
            color="black", linewidth=1.8, zorder=3)

    ax.plot(mean, y_pos, marker="o", color="black", markersize=4.5, zorder=4)

    ax.plot([vmin, q1], [y_pos, y_pos], color="black", linewidth=0.8)
    ax.plot([q3, vmax], [y_pos, y_pos], color="black", linewidth=0.8)


def style_axes(ax):
    ax.set_facecolor("#EAEAF2")
    ax.figure.set_facecolor("white")

    ax.grid(axis="x", color="white", linewidth=0.8, which="major", zorder=0)
    ax.grid(axis="x", color="white", linewidth=0.4, which="minor", zorder=0)
    ax.grid(axis="y", visible=False)

    for spine in ax.spines.values():
        spine.set_color("#CCCCCC")
        spine.set_linewidth(0.6)

    ax.tick_params(axis="both", which="both", length=0)
    ax.tick_params(axis="x", labelsize=8, labelcolor="#555555")
    ax.tick_params(axis="y", labelsize=9, labelcolor="#333333")


# --- Numeric variable box plots ---
for var in numeric_vars:
    n_companies = len(companies)
    fig_h = max(1.2 + n_companies * 0.55, 2.5)
    fig, ax = plt.subplots(figsize=(10, fig_h))

    y_positions = []
    y_labels = []
    plotted = False
    fallback_idx = 0

    for i, company in enumerate(companies):
        df = company_data[company]
        row = df[df["variable"] == var]
        if row.empty:
            continue
        stats = row.iloc[0]
        if pd.isna(stats["25th_percentile"]):
            continue

        n = int(stats["count"])
        y_pos = n_companies - i
        cl, cd = get_colors(company, fallback_idx)
        if company not in company_colors:
            fallback_idx += 1
        draw_horizontal_box(ax, y_pos, stats, cl, cd)
        y_positions.append(y_pos)
        y_labels.append(f"{company} (n={n})")
        plotted = True

    if not plotted:
        plt.close(fig)
        print(f"  Skipped {var} — no data")
        continue

    ax.set_xscale("log")
    ax.set_yticks(y_positions)
    ax.set_yticklabels(y_labels)
    ax.set_ylabel("Company", fontsize=9, color="#555555")
    ax.set_xlabel(var, fontsize=9, color="#555555")

    ax.set_ylim(min(y_positions) - 0.6, max(y_positions) + 0.6)

    style_axes(ax)

    fig.tight_layout()
    out_path = os.path.join(output_dir, f"{var}_boxplot.png")
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {out_path}")


# --- Boolean variable bar charts ---
for var in boolean_vars:
    n_companies = len(companies)
    fig_h = max(1.2 + n_companies * 0.55, 2.5)
    fig, ax = plt.subplots(figsize=(10, fig_h))

    y_positions = []
    y_labels = []
    vals = []
    bar_colors = []
    fallback_idx = 0

    for i, company in enumerate(companies):
        df = company_data[company]
        row = df[df["variable"] == var]
        if row.empty or "pct_true" not in row.columns:
            continue
        pct = row.iloc[0].get("pct_true", np.nan)
        if pd.isna(pct):
            continue
        n = int(row.iloc[0]["count"])
        y_pos = n_companies - i
        y_positions.append(y_pos)
        y_labels.append(f"{company} (n={n})")
        vals.append(pct)
        cl, _ = get_colors(company, fallback_idx)
        if company not in company_colors:
            fallback_idx += 1
        bar_colors.append(cl)

    if not vals:
        plt.close(fig)
        print(f"  Skipped {var} — no data")
        continue

    ax.barh(y_positions, vals, color=bar_colors, edgecolor="black",
            height=0.45, linewidth=0.8)

    for yp, v in zip(y_positions, vals):
        ax.text(v + 1.2, yp, f"{v:.1f}%", va="center", fontsize=8, color="#333333")

    ax.set_yticks(y_positions)
    ax.set_yticklabels(y_labels)
    ax.set_ylabel("Company", fontsize=9, color="#555555")
    ax.set_xlabel("% True", fontsize=9, color="#555555")
    ax.set_xlim(0, 110)
    ax.set_ylim(min(y_positions) - 0.6, max(y_positions) + 0.6)

    style_axes(ax)

    fig.tight_layout()
    out_path = os.path.join(output_dir, f"{var}_bar.png")
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {out_path}")

print(f"\nAll figures saved to {output_dir}/")