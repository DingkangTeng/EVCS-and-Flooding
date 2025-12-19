import os, sys
import pandas as pd
import numpy as np
import seaborn as sns

sys.path.append(".") # Set path to the roots

from _plot import plt, BAR_COLORS, NOTE_SIZE
from analysis.__readNode import readNode
from analysis.__calRatio import calRatio
from analysis.__setting import AColumns

def sensativeAnalysis(root: str, buffer: list[str], figsize: str = "D") -> None:
    allDf = np.empty([len(buffer)], dtype=pd.DataFrame)
    A_BEFORE = np.empty([3], dtype=object)
    A_AFTER = np.empty([3], dtype=object)
    for i, analysisType in enumerate(["popStatic", "popDynamic", "POI"]):
        A_ACC_BEFORE, A_ACC_AFTER = AColumns(analysisType, "accessibility")
        A_BEFORE[i] = A_ACC_BEFORE[-1]
        A_AFTER[i] = A_ACC_AFTER[-1]
    
    ratio = np.empty(0)
    for i, sub in enumerate(buffer):
        path = os.path.join(root, sub, "city.csv")
        subDf, _, _ = readNode(path)
        subDf = subDf[A_BEFORE.tolist() + A_AFTER.tolist() + ["city"]]
        subDf, ratio = calRatio(subDf, A_BEFORE, A_AFTER)
        subDf: pd.DataFrame = subDf[ratio.tolist() + ["city"]]
        subDf["buffer"] = sub
        allDf[i] = subDf
    
    df = pd.concat(allDf, ignore_index=True)

    # 使用lineplot，误差显示为带状的置信区间
    fig, ax = plt.figure(figsize)
    for y in ratio:
        subDf = df#[df[y] != 0]
        sns.lineplot(
            data=subDf,
            ax=ax,
            x="buffer", y=y,
            errorbar="ci",    # 或 "se", "ci"
            err_style="band", # 可选 "band" 或 "bars"
            marker="o",
            linewidth=2,
            label=y
        )

    plt.plot()

    return

if __name__ == "__main__":
    sensativeAnalysis(r"C:\\0_PolyU\\test\\", ["1km", "3km", "5km"])