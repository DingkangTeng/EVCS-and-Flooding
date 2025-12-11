__all__ = [
    "legend",
    "yticks", "xticks", "ticklabel_format",
    "plot", "figure", "subplot",
    "standAxisName"
]

import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.axes import Axes
from matplotlib.text import Text

from .__setting import FIG_SIZE

# plt original function
from matplotlib.pyplot import (
    legend,
    yticks, xticks, ticklabel_format
)

# Print or save fig
def plot(path: str = "", **kwgs) -> None:
    plt.tight_layout()

    if path == "":
        plt.show()
    else:
        plt.savefig(path, **kwgs)

    return plt.close()

# Initial fig
def figure(figsize: str) -> tuple[Figure, Axes]:
    fig = plt.figure(figsize=getattr(FIG_SIZE, figsize))
    ax = fig.subplots()

    return fig, ax

# Initial subplot in one fig
class subplot:
    __slots__ = ["__fig", "__axs"]

    def __init__(self, figsize: str, y: int, x: int, widthRatios: list[int], legend: bool = True) -> None:
        from matplotlib.gridspec import GridSpec

        self.__fig = plt.figure(figsize=getattr(FIG_SIZE, figsize))
        if legend:
            heightRatios = [max(1, 8//y)] * y + [1]
            gs = GridSpec(y+1, x, height_ratios=heightRatios, width_ratios=widthRatios)
        else:
            gs = GridSpec(y, x, width_ratios=widthRatios)

        self.__axs: list[Axes] = [plt.subplot(gs[i, j]) for i in range(y) for j in range(x)]

    @property
    def fig(self) -> Figure:
        return self.__fig
    
    @property
    def axs(self) -> list[Axes]:
        return self.__axs
    
# Change axis name
def standAxisName(ax: Axes, axis: str, standard: dict) -> None:
    def getlist(fun: list[Text]) -> list[str]:
        return [standard.get(tick.get_text(), tick.get_text()).capitalize() for tick in fun]
    
    if axis == "x":
        ax.set_xticks(ax.get_xticks())
        ax.set_xticklabels(
            getlist(ax.get_xticklabels())
        )

    elif axis == "y":
        ax.set_yticks(ax.get_yticks())
        ax.set_yticklabels(
            getlist(ax.get_yticklabels())
        )