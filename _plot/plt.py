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
def plot(path: str = "", saveName: str = "", fig: Figure | None = None, **kwgs) -> None:
    from os.path import join
    fig = fig if fig is not None else plt.gcf()
    
    fig.tight_layout()

    if path == "":
        fig.show()
    else:
        fig.savefig(
            join(path, "defaultName.jpg") if saveName == "" else join(path, saveName),
            **kwgs
        )

    return plt.close(fig)

# Initial fig
def figure(figsize: str) -> tuple[Figure, Axes]:
    fig = plt.figure(figsize=getattr(FIG_SIZE, figsize))
    ax = fig.subplots()

    return fig, ax

# Initial subplot in one fig
class subplot:
    __slots__ = ["__fig", "__axs", "sharex", "sharey", "xy"]

    def __init__(
        self,
        figsize: str,
        y: int, x: int,
        heightRatios: list[int] | None = None, widthRatios: list[int] | None = None,
        legend: bool = True,
        sharex: bool = False, sharey: bool = False,
        keepyticks: bool = False, keepxticks: bool = False
    ) -> None:
        from matplotlib.gridspec import GridSpec

        self.__fig = plt.figure(figsize=getattr(FIG_SIZE, figsize))
        self.sharex = sharex
        self.sharey = sharey
        self.xy = (x, y, legend)

        if legend:
            if heightRatios is not None:
                heightRatios = heightRatios + [0]
            else:
                heightRatios = [max(1, 8//y)] * y + [1]
            gs = GridSpec(y+1, x, height_ratios=heightRatios, width_ratios=widthRatios)
        else:
            gs = GridSpec(y, x, height_ratios=heightRatios, width_ratios=widthRatios)

        # self.__axs: list[Axes] = [plt.subplot(gs[i, j]) for i in range(y) for j in range(x)]
        self.__axs: list[Axes] = []
        for i in range(y):
            for j in range(x):
                if i == 0 and j == 0: ax = plt.subplot(gs[0, 0])
                else:
                    ax = plt.subplot(
                        gs[i, j],
                        sharex=self.__axs[0] if sharex else None,
                        sharey=self.__axs[0] if sharey else None
                    )

                # Delete inner ticks
                if not keepxticks and sharex and i != y - 1: ax.tick_params(axis='x', labelbottom=False)
                if not keepyticks and sharey and j != 0: ax.tick_params(axis='y', labelleft=False)

                self.__axs.append(ax)

        return

    @property
    def fig(self) -> Figure:
        # Delete inner axis name
        if self.sharex:
            for ax in self.__axs[:-self.xy[0]*(self.xy[2]+1)]:
                ax.set_xlabel("")
        if self.sharey:
            for ax in [x for i, x in enumerate(self.__axs) if i % self.xy[0] != 0]:
                ax.set_ylabel("")

        return self.__fig
    
    @property
    def axs(self) -> list[Axes]:
        return self.__axs
    
    def plot(self, path: str = "", saveName: str = "", **kwgs) -> None:
        plot(path, saveName, self.__fig, **kwgs)
    
# Change axis name
def standAxisName(ax: Axes, axis: str, standard: dict) -> None:
    def getlist(fun: list[Text]) -> list[str]:
        return [
            standard.get(
                tick.get_text(), tick.get_text()
            ).capitalize().replace("poi", "POI") for tick in fun
        ]
    
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