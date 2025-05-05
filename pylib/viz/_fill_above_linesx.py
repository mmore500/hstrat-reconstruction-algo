from matplotlib import axes as mpl_axes
from matplotlib import pyplot as plt


# adapted from https://github.com/mwaskom/seaborn/issues/2410#issuecomment-753474050
def fill_above_linesx(
    ax: mpl_axes.Axes=None,
    alpha: float=0.2,
    fill_to: float=1.0,
    **kwargs,
) -> None:
    if ax is None:
        ax = plt.gca()
    for line in ax.lines:
        x, y = line.get_xydata().T
        ax.fill_betweenx(
            y,
            x,
            fill_to,
            alpha=alpha,
            **{
                "color": line.get_color(),
                **kwargs,
            },
        )
