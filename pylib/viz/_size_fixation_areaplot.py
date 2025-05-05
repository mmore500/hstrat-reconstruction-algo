import typing

import pandas as pd
import seaborn as sns

from ._fill_above_lines import fill_above_lines
from ._fill_under_lines import fill_under_lines


def size_fixation_areaplot(
    fixprobs_df: pd.DataFrame,
    x: str,
    y: str,
    col: str,
    hue: str,
    hue_order: typing.List[str],
    palette: typing.Optional[str] = "tab10",
    seed: int = 1,
    xscale: str = "log",
    **kwargs: dict,
) -> sns.FacetGrid:
    palette = sns.color_palette(palette)
    g = sns.relplot(
        data=fixprobs_df[fixprobs_df[hue] == hue_order[1]],
        x=x,
        y=y,
        col=col,
        hue=hue,
        hue_order=hue_order,
        err_kws={"alpha": 0.5},
        kind="line",
        orient="x",
        palette=palette,
        seed=seed,
        **kwargs,
    )
    g.set(xscale=xscale, ylim=(-0.02, 1.02))

    for ax in g.axes.flat:
        fill_above_lines(ax, color=palette[0])
        fill_under_lines(ax)

    return g
