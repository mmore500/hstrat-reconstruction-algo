from matplotlib import colors as mpl_colors
from matplotlib import ticker as mpl_ticker
from matplotlib.axes import Axes as mpl_Axes
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl
import seaborn as sns


def _make_cmap():
    # From dark blue to lighter blue
    colors_top = plt.cm.Blues(np.linspace(1, 0.4, 128))
    # From lighter red to white
    colors_bottom = plt.cm.Reds(np.linspace(0.3, 0, 128))

    colors_combined = np.vstack((colors_top, colors_bottom))

    return mpl_colors.LinearSegmentedColormap.from_list(
        "custom_cmap",
        list(colors_combined),
    )


def _munge_fixprobs_df(
    fixprobs_df: pd.DataFrame,
    genotype: str,
    index: str,
    on: str,
    values: str,
) -> pd.DataFrame:
    return (
        pl.DataFrame(fixprobs_df)
        .filter(pl.col("genotype") == genotype)
        .with_columns(
            pl.col("population size")
            .cast(pl.Float64)
            .map_elements(
                lambda x: f"{x:.2e}",
                return_dtype=pl.String,
            ),
        )
        .pivot(
            index=index,
            on=on,
            values=values,
            aggregate_function="mean",
        )
        .sort(
            index,
            descending=True,
        )
        .to_pandas()
        .set_index(
            index,
        )
    )


def size_fixation_heatmap(
    fixprobs_df: pd.DataFrame,
    genotype: str,
    index: str,
    on: str,
    values: str,
) -> mpl_Axes:

    hmdf = _munge_fixprobs_df(
        fixprobs_df,
        genotype=genotype,
        index=index,
        on=on,
        values=values,
    )

    # Assume hmdf is your DataFrame and custom_cmap is your colormap
    ax = sns.heatmap(
        hmdf,
        cmap=_make_cmap(),
        annot=True,  # Optional: Add annotations inside the squares
        fmt=".2f",  # Formatting numbers to 2 decimal places
        linewidths=0.5,  # Line width between cells
        vmin=0.0,
        vmax=1.0,  # Fixation probability color range
        cbar_kws={
            "label": "",
            "shrink": 0.7,
        },
    )

    # Invert colorbar and add labels as before
    cbar = plt.gcf().axes[-1]  # Access the colorbar
    cbar_label_top = "normo\nmutate\nfavored"
    cbar_label_bottom = "\nhyper\nmutate\nfavored"
    plt.text(
        1.05,
        1.2,
        cbar_label_bottom,
        ha="center",
        va="center",
        transform=cbar.transAxes,
        fontsize=12,
    )
    plt.text(
        1.05,
        -0.15,
        cbar_label_top,
        ha="center",
        va="center",
        transform=cbar.transAxes,
        fontsize=12,
    )
    plt.gca().set_facecolor("gainsboro")
    plt.gca().axes.get_xaxis().set_visible(False)

    # Get the position of the current axes
    pos = ax.get_position()

    # Create a new dummy axis underneath the heatmap with minimal height
    delta = 0.0  # Small height for the dummy axis
    new_ax = ax.figure.add_axes([pos.x0, pos.y0 - delta, pos.width, delta])

    # Set the x-axis of the dummy axis to logarithmic scale
    new_ax.set_xscale("log")

    # Assume hmdf.columns are your x-axis values
    xvals = hmdf.columns.astype(float)
    xmin = xvals.min()
    xmax = xvals.max()
    new_ax.set_xlim(xmin, xmax)

    # Use built-in LogLocator and LogFormatterMathtext for base 10 labels
    new_ax.xaxis.set_major_locator(mpl_ticker.LogLocator(base=10))
    new_ax.xaxis.set_major_formatter(
        mpl_ticker.LogFormatterMathtext(base=10),
    )

    # Hide the y-axis of the dummy axis
    new_ax.get_yaxis().set_visible(False)

    # Hide spines of the dummy axis
    for spine in new_ax.spines.values():
        spine.set_visible(False)

    # Adjust layout to make room for the new x-axis labels
    plt.subplots_adjust(bottom=0.11)

    return ax
