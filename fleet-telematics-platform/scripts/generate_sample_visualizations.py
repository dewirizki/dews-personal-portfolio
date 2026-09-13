"""Generates the two sample-output graphics embedded on the portfolio site
(assets/images/fleet-*.png) from synthetic-but-realistic data shaped exactly
like what services/stream_processing writes to ClickHouse.

These are illustrative captures, not a live dashboard screenshot -- the
live Streamlit/Grafana views (services/dashboard, infra/grafana) are the
real interactive artifacts; this script exists so the portfolio write-up
has a concrete "here's what the pipeline actually produces" image without
requiring a reviewer to stand up the full stack.

Run:
    poetry run python ../../scripts/generate_sample_visualizations.py
(from services/stream_processing, so `h3` resolves from that env) or with
any Python environment that has h3, matplotlib, and numpy installed.
"""

from __future__ import annotations

import random
from pathlib import Path

import h3
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "assets" / "images"

# Site design tokens (assets/style.css :root) -- reused here so the
# generated graphics sit inside the same visual system as the rest of the
# portfolio rather than introducing an unrelated palette.
ACCENT_SOFT = "#DCEBE6"
ACCENT = "#1F6F5C"
ACCENT_STRONG = "#134338"
TEXT = "#151A17"
TEXT_MUTED = "#57615B"
BORDER = "#DBE1DC"
SURFACE = "#FFFFFF"
WARN = "#A9662A"
STATUS_WARNING = "#fab219"
STATUS_CRITICAL = "#d03b3b"

random.seed(7)
np.random.seed(7)


def _sequential_cmap():
    from matplotlib.colors import LinearSegmentedColormap

    # One hue, light -> dark (sequential = magnitude, per dataviz skill's
    # color formula) -- anchored to the site's own accent-soft/accent-strong
    # tokens rather than an arbitrary ramp.
    return LinearSegmentedColormap.from_list("accent_sequential", [ACCENT_SOFT, ACCENT, ACCENT_STRONG])


def generate_density_heatmap() -> None:
    """H3-r9 vehicle density heatmap around a lower-Manhattan sample area --
    shaped exactly like a query against h3_density_5min_current."""
    center_lat, center_lon = 40.7128, -74.0060
    center_cell = h3.latlng_to_cell(center_lat, center_lon, 9)
    cells = list(h3.grid_disk(center_cell, 7))

    # Synthetic density: a couple of "hot" clusters (e.g. transit hubs) plus
    # ambient background traffic -- mirrors real fleet data's clustered,
    # not uniform, spatial distribution.
    hot_cells = random.sample(cells, k=3)
    counts = {}
    for cell in cells:
        base = np.random.poisson(3)
        if cell in hot_cells:
            base += np.random.poisson(22)
        counts[cell] = base

    max_count = max(counts.values())
    cmap = _sequential_cmap()

    fig, ax = plt.subplots(figsize=(8, 7), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    patches, colors = [], []
    for cell, count in counts.items():
        boundary = h3.cell_to_boundary(cell)
        polygon_xy = [(lon, lat) for lat, lon in boundary]
        patches.append(Polygon(polygon_xy, closed=True))
        colors.append(count / max_count)

    collection = PatchCollection(patches, cmap=cmap, edgecolor=BORDER, linewidths=0.6)
    collection.set_array(np.array(colors))
    ax.add_collection(collection)

    # Direct label only the top-3 hottest cells -- selective labeling, not
    # a number on every hexagon.
    for cell in hot_cells:
        lat, lon = h3.cell_to_latlng(cell)
        ax.annotate(
            str(counts[cell]),
            (lon, lat),
            ha="center",
            va="center",
            fontsize=9,
            fontweight="bold",
            color=SURFACE,
        )

    ax.autoscale_view()
    ax.set_aspect(1.32)  # rough latitude correction for the display
    ax.axis("off")
    ax.set_title(
        "Vehicle density by H3 cell (resolution 9) — 5-minute window",
        fontsize=13,
        color=TEXT,
        fontweight="600",
        loc="left",
        pad=14,
    )
    ax.text(
        0.0, -0.04,
        "Sample output — h3_density_5min_current, synthetic data shaped like a live run",
        transform=ax.transAxes,
        fontsize=8.5,
        color=TEXT_MUTED,
    )

    cbar = fig.colorbar(collection, ax=ax, fraction=0.035, pad=0.02)
    cbar.set_label("vehicles / cell", color=TEXT_MUTED, fontsize=9)
    cbar.ax.tick_params(labelsize=8, colors=TEXT_MUTED)
    cbar.outline.set_edgecolor(BORDER)

    fig.tight_layout()
    out_path = OUTPUT_DIR / "fleet-telematics-h3-density.png"
    fig.savefig(out_path, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def generate_speed_anomaly_chart() -> None:
    """One vehicle's speed trace over a 30-minute window against the local
    H3-cell mean +/- stddev band, with points flagged the same way
    detect_speed_anomalies() flags them (|z| >= 3 WARNING, |z| >= 5 CRITICAL).
    """
    minutes = np.arange(0, 30, 0.5)
    local_mean = 11.0 + 1.2 * np.sin(minutes / 6)  # slow neighborhood drift
    local_stddev = 1.8 + 0.15 * np.cos(minutes / 9)

    vehicle_speed = local_mean + np.random.normal(0, 1.0, size=minutes.shape)
    # Inject a hard-braking anomaly (CRITICAL) and a speeding anomaly (WARNING).
    vehicle_speed[24] = local_mean[24] - 5.8 * local_stddev[24]   # CRITICAL: harsh braking
    vehicle_speed[25] = local_mean[25] - 2.1 * local_stddev[25]
    vehicle_speed[40] = local_mean[40] + 3.4 * local_stddev[40]   # WARNING: speeding relative to local traffic
    vehicle_speed = np.clip(vehicle_speed, 0, None)

    z_scores = (vehicle_speed - local_mean) / local_stddev
    warning_mask = (np.abs(z_scores) >= 3.0) & (np.abs(z_scores) < 5.0)
    critical_mask = np.abs(z_scores) >= 5.0

    fig, ax = plt.subplots(figsize=(9, 4.6), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    ax.fill_between(
        minutes, local_mean - 3 * local_stddev, local_mean + 3 * local_stddev,
        color=ACCENT_SOFT, alpha=0.6, label="±3σ of local H3-cell window (normal band)", linewidth=0,
    )
    ax.plot(minutes, local_mean, color=ACCENT, linewidth=1.5, linestyle="--", label="Local cell mean speed")
    ax.plot(minutes, vehicle_speed, color=TEXT, linewidth=2, label="Vehicle MTA-B41-014 speed")

    ax.scatter(
        minutes[warning_mask], vehicle_speed[warning_mask],
        s=70, color=STATUS_WARNING, edgecolor=TEXT, zorder=5, label="WARNING anomaly (|z| ≥ 3)",
    )
    ax.scatter(
        minutes[critical_mask], vehicle_speed[critical_mask],
        s=90, color=STATUS_CRITICAL, edgecolor=TEXT, zorder=5, marker="X", label="CRITICAL anomaly (|z| ≥ 5)",
    )

    ax.set_xlabel("Minutes into window", color=TEXT_MUTED, fontsize=9.5)
    ax.set_ylabel("Speed (m/s)", color=TEXT_MUTED, fontsize=9.5)
    ax.tick_params(colors=TEXT_MUTED, labelsize=8.5)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(BORDER)
    ax.grid(axis="y", color=BORDER, linewidth=0.6, alpha=0.7)
    ax.set_axisbelow(True)

    ax.set_title(
        "Speed-anomaly detection: vehicle speed vs. local H3-cell traffic",
        fontsize=13, color=TEXT, fontweight="600", loc="left", pad=28,
    )
    fig.text(
        0.085, 0.895,
        "Sample output — speed_anomalies stream-stream join, synthetic data shaped like a live run",
        fontsize=8.5, color=TEXT_MUTED,
    )

    legend = ax.legend(
        loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=3, frameon=False, fontsize=8.2,
        labelcolor=TEXT_MUTED,
    )

    fig.tight_layout(rect=(0, 0.02, 1, 1))
    out_path = OUTPUT_DIR / "fleet-telematics-speed-anomaly.png"
    fig.savefig(out_path, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    generate_density_heatmap()
    generate_speed_anomaly_chart()


if __name__ == "__main__":
    main()
