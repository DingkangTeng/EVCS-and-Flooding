import sys, os
import numpy as np

sys.path.append(".") # Set path to the roots

from _plot import plt
from analysis.__readNode import readNode
from analysis.__setting import A_POI, A_POP
from analysis.__statisticalDiff import Wilcoxon

def aggerateAnalysis(
    path: str, analysisType: str,
    savePath: str,
    accOrEquity : str = "accessibility",
    minEvcsNum: int = 10
) -> None:
    """
    Generate ECDF plot.

    Parameters
    ----------
    analysisType : {'POI', 'popStatic', 'popDynamic'}
        'POI' analysis the relationship between different POI. 'popStatic' analysis the relationship
        between gender demograph, age demograph and all population. 'popDynamic' analysis the relatiopship
        between all static population and all dynaic population.
    """
    if analysisType == "POI":
        A_BEFORE = list(A_POI.before)
        A_AFTER = list(A_POI.after)
    elif analysisType == "popStatic":
        A_BEFORE = list(A_POP.staticBefore)
        A_AFTER = list(A_POP.staticAfter)
    elif analysisType == "popDynamic":
        A_BEFORE = list(A_POP.dynamicBefore)
        A_AFTER = list(A_POP.dynamicAfter)
    else:
        raise RuntimeError("Unsupport analysis type {}.".format(analysisType))
    
    if accOrEquity == "equity":
        A_BEFORE = ["{}_Gini".format(x) for x in A_BEFORE]
        A_AFTER = ["{}_Gini".format(x) for x in A_AFTER]

    df, n, nc = readNode(path, minEvcsNum)
    scale = os.path.basename(path).split('.')[0]
    addCol = ["iso3", "city"] if scale == "city" else ["iso3"]
    df = df[A_BEFORE + A_AFTER + addCol]
    savePath = os.path.join(savePath, analysisType)
    if not os.path.exists(savePath): os.makedirs(savePath)

    ratio = np.ndarray([len(A_BEFORE)], dtype=object)
    zeroCounts = np.ndarray([len(A_BEFORE)], dtype=np.uint16)
    nonZeroCounts = np.ndarray([len(A_BEFORE)], dtype=np.uint16)
    # results = np.ndarray([len(A_BEFORE)], dtype=pd.DataFrame)
    for i, a in enumerate(A_BEFORE):
        col = "{}_changeRatio".format(a)
        ratio[i] = col
        df[col] = (df[A_AFTER[i]] / df[a] - 1) * 100
        allRecord = df[df[col].notna()]
        affected = allRecord[allRecord[col] != 0]
        print(
            f"In {a}, {affected.shape[0]} nodes/region ({affected.shape[0] / allRecord.shape[0] * 100:.2f}%) are affected by flooding."
        )
        if accOrEquity == "equity":
            decrease = df[df[col] > 0].shape[0]
            print(
                f"In {a}, {decrease} nodes/region ({decrease / affected.shape[0] * 100:.2f}%) in the affecred nodes have a decrease in equity."
            )

        # Statistic 0 and non-0
        zeroCount = (df[col] == 0).sum()
        zeroCounts[i] = zeroCount
        nonZeroCounts[i] = allRecord.shape[0] - zeroCount
    
    # Save change ratio
    df.to_csv(os.path.join(savePath, f"{scale}_{accOrEquity}_results.csv"))
    # Plot
    subplot = plt.subplot("D", 1, 2, [1, 4])
    ax1 = subplot.axs[1]
    ax2 = subplot.axs[0]

    ## Right: non-0
    for group in ratio:
        groupData = df[group][(df[group] != 0) & df[group].notna()]
        if len(groupData) > 0:
            # ECDF
            x = np.sort(groupData)
            y = np.arange(1, len(x)+1) / len(x)
            ax1.plot(x, y, label=group, alpha=0.7, linewidth=2)
    ax1.set_xlabel("Change Ratio (%)")
    ax1.set_ylabel("Cumulative Probability")
    ax1.yaxis.tick_right()
    ax1.yaxis.set_label_position("right")
    
    ## Left: proportion of 0 bar
    yPos = np.arange(len(ratio))
    
    totalCounts = [zero + non_zero for zero, non_zero in zip(zeroCounts, nonZeroCounts)]
    zeroPercent = [zero / total * 100 for zero, total in zip(zeroCounts, totalCounts)]
    nonZeroPercent = [non_zero / total * 100 for non_zero, total in zip(nonZeroCounts, totalCounts)]
    
    ## Stacked horizontal bar
    bars1 = ax2.barh(yPos, zeroPercent, height=0.6, label="Zero Values", alpha=0.7)
    bars2 = ax2.barh(yPos, nonZeroPercent, height=0.6, left=zeroPercent, label="Non-zero Values", alpha=0.7)
    
    ## Label
    for i, (zp, nzp) in enumerate(zip(zeroPercent, nonZeroPercent)):
        ax2.text(zp/2, i, f"{zp:.2f}%", ha="center", va="center", fontsize=8, fontweight="bold") if zp != 0 else None
        ax2.text(zp + nzp/2, i, f"{nzp:.2f}%", ha="center", va="center", fontsize=8, fontweight="bold")
    ax2.set_xlabel("Percentage (%)")
    ax2.set_yticks(yPos)
    ax2.set_yticklabels(ratio, rotation=45)
    ax2.grid(True, alpha=0.3, axis='x')

    subplot.fig.legend(loc="lower center", ncol=2)

    plt.plot(os.path.join(savePath, f"{scale}_{accOrEquity}.jpg"))

    # Wilcoxon
    wilcoxon = Wilcoxon(ratio, df, f"{analysisType} {accOrEquity}")
    if savePath != "":
        wilcoxon.to_csv(
            os.path.join(savePath, f"{scale}_{accOrEquity}_wilcoxon.csv"),
            encoding="utf-8", index=False
        )

    return

# Debug
if __name__ == "__main__":
    aggerateAnalysis(r"C:\\0_PolyU\\test\\city.csv", "popStatic", r"C:\\0_PolyU\\test")
    aggerateAnalysis(r"C:\\0_PolyU\\test\\city.csv", "popDynamic", r"C:\\0_PolyU\\test")
    aggerateAnalysis(r"C:\\0_PolyU\\test\\iso3.csv", "popStatic", r"C:\\0_PolyU\\test")
    aggerateAnalysis(r"C:\\0_PolyU\\test\\iso3.csv", "popDynamic", r"C:\\0_PolyU\\test")

    aggerateAnalysis(r"C:\\0_PolyU\\test\\city.csv", "popStatic", r"C:\\0_PolyU\\test", "equity")
    aggerateAnalysis(r"C:\\0_PolyU\\test\\city.csv", "popDynamic", r"C:\\0_PolyU\\test", "equity")
    aggerateAnalysis(r"C:\\0_PolyU\\test\\iso3.csv", "popStatic", r"C:\\0_PolyU\\test", "equity")
    aggerateAnalysis(r"C:\\0_PolyU\\test\\iso3.csv", "popDynamic", r"C:\\0_PolyU\\test", "equity")
    
    aggerateAnalysis(r"C:\\0_PolyU\\test\\city.csv", "POI", r"C:\\0_PolyU\\test")
    aggerateAnalysis(r"C:\\0_PolyU\\test\\iso3.csv", "POI", r"C:\\0_PolyU\\test")

    aggerateAnalysis(r"C:\\0_PolyU\\test\\city.csv", "POI", r"C:\\0_PolyU\\test", "equity")
    aggerateAnalysis(r"C:\\0_PolyU\\test\\iso3.csv", "POI", r"C:\\0_PolyU\\test", "equity")