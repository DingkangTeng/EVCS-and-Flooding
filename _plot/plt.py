__all__ = [
    "close", "legend", "ticklabel_format",
    "plot", "figure"
    ]

import matplotlib.pyplot as __plt
from matplotlib.figure import Figure
from matplotlib.axes import Axes

from .setting import FIG_SIZE

# plt original function
from matplotlib.pyplot import (
    close, legend,
    yticks, xticks, ticklabel_format
)

# Print or save fig
def plot(path: str = "", **kwgs) -> None:
    __plt.tight_layout()

    if path == "":
        __plt.show()
    else:
        __plt.savefig(path, **kwgs)

    return

# Initial fig
def figure(figsize: str) -> tuple[Figure, Axes]:
    fig = __plt.figure(figsize=getattr(FIG_SIZE, figsize))
    ax = fig.subplots()

    return fig, ax