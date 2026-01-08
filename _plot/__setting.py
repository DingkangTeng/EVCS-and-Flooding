import matplotlib.pyplot as plt
from dataclasses import dataclass

# Fig size
LABEL_SIZE = 28
TICK_SIZE = int(LABEL_SIZE * 0.9)
NOTE_SIZE = int(LABEL_SIZE * 0.7)
@dataclass
class __FIG_SIZE:
    D: tuple[int, int] = (12, 12)   # Default
    D31: tuple[int, int] = (8, 8) # Default 3*3
    H: tuple[int, int] = (12, 24)   # High
    N: tuple[int, int] = (12, 6)   # Narrow
    W: tuple[int, int] = (24, 12)    # Wide
    WN31: tuple[int, int] = (24, 10)    # Wide Narrow
FIG_SIZE = __FIG_SIZE()