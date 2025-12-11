import matplotlib.pyplot as plt
from dataclasses import dataclass

# Fig size
LABEL_SIZE = 24
TICK_SIZE = int(LABEL_SIZE * 0.9)
NOTE_SIZE = int(LABEL_SIZE * 0.6)
@dataclass
class __FIG_SIZE:
    D: tuple[int, int] = (12, 12)   # Default
    H: tuple[int, int] = (12, 24)   # High
    N: tuple[int, int] = (12, 6)   # Narrow
    W: tuple[int, int] = (24, 12)    # Wide
    SL: tuple[int, int] = (6, 12)   # Slim
FIG_SIZE = __FIG_SIZE()