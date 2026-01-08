import sys, os
import numpy as np
from matplotlib.axes import Axes

sys.path.append(".") # Set path to the roots

from _plot import plt, BAR_COLORS, NOTE_SIZE
from analysis.__readNode import readNode
from analysis.__setting import STAND_NAME, AColumns
from analysis.__statisticalDiff import Wilcoxon
from analysis.__calRatio import calRatio

def popAndPOIAnalysis(
    path: str, analysisType: str,
    savePath: str, axs: Axes | None = None,
    accOrEquity: str = "accessibility",
    minEvcsNum: int = 0
) -> None:
    """
    Generate ECDF plot.

    Parameters
    ----------
    analysisType : {'POI', 'popStatic', 'popDynamic', 'pop'}
        'POI' analysis the relationship between different POI. 'popStatic' analysis the relationship
        between gender demograph, age demograph and all population. 'popDynamic' analysis the relatiopship
        between all static population and all dynaic population.
    """
    A_BEFORE, A_AFTER = AColumns(analysisType, accOrEquity)

    df, n, nc = readNode(path, minEvcsNum)
    scale = os.path.basename(path).split('.')[0]
    addCol = ["iso3", "city"] if scale == "city" else ["iso3"]
    df = df[A_BEFORE + A_AFTER + addCol]
    savePath = os.path.join(savePath, analysisType)
    if not os.path.exists(savePath): os.makedirs(savePath)

    df, ratio, zeroCounts, nonZeroCounts = calRatio(
        df, A_BEFORE, A_AFTER, True, True if accOrEquity == "equity" else False
    )
    
    # Save change ratio
    print("\n")
    df.to_csv(os.path.join(savePath, f"{scale}_{accOrEquity}_results.csv"))
    # Plot
    subplot = plt.subplot("WN31", 1, 3, [1, 3, 4])
    ax1 = subplot.axs[1]
    ax2 = subplot.axs[0]
    ax3 = subplot.axs[2]

    ## Right: non-0
    group: str
    colors = BAR_COLORS[0] + BAR_COLORS[2]
    i = 0
    for group in ratio:
        groupData = df[group][(df[group] != 0) & df[group].notna()]
        if len(groupData) > 0:
            # ECDF
            x = np.sort(groupData)
            y = np.arange(1, len(x)+1) / len(x)
            ax1.plot(
                x, y,
                label=STAND_NAME.get(group, group).capitalize().replace("\n", " ").replace("poi", "POI"),
                color=colors[i],
                linewidth=2
            )
            i += 1

    ax1.set_xlabel("Change Ratio (%)")
    ax1.set_ylabel("Cumulative Probability")
    # ax1.yaxis.tick_right()
    # ax1.yaxis.set_label_position("right")
    
    ## Left: proportion of 0 bar
    yPos = np.arange(len(ratio))
    
    totalCounts = [zero + non_zero for zero, non_zero in zip(zeroCounts, nonZeroCounts)]
    zeroPercent = [zero / total * 100 for zero, total in zip(zeroCounts, totalCounts)]
    nonZeroPercent = [non_zero / total * 100 for non_zero, total in zip(nonZeroCounts, totalCounts)]
    
    ## Stacked horizontal bar
    ax2.barh(yPos, zeroPercent, height=0.6, label="Zero Values", alpha=0.7)
    ax2.barh(yPos, nonZeroPercent, height=0.6, left=zeroPercent, label="Non-zero Values", alpha=0.7)
    
    ## Label
    for i, (zp, nzp) in enumerate(zip(zeroPercent, nonZeroPercent)):
        ax2.text(zp/2, i, f"{zp:.2f}%", ha="center", va="center", fontsize=NOTE_SIZE, fontweight="bold") if zp != 0 else None
        ax2.text(zp + nzp/2, i, f"{nzp:.2f}%", ha="center", va="center", fontsize=NOTE_SIZE, fontweight="bold")
    ax2.set_xlabel("Percentage (%)")
    ax2.set_yticks(yPos)
    ax2.set_yticklabels(ratio, rotation=45)
    ax2.grid(True, alpha=0.3, axis='x')

    subplot.fig.legend(loc="lower center", ncol=np.ceil(len(ratio)/2)+1)
    plt.standAxisName(ax2, 'y', STAND_NAME)

    # Wilcoxon
    wilcoxon = Wilcoxon(
        ratio,
        df,
        f"{analysisType} {accOrEquity}",
        os.path.join(savePath, f"{scale}_{accOrEquity}_wilcoxon.csv") if savePath != "" else ""
    )

    # Draw box plot
    # fig, ax = plt.figure("S1") if analysisType != "popDynamic" else plt.figure("S12")
    import seaborn as sns
    sns.boxplot(
        data=df[ratio],
        ax = ax3,
        showfliers=False,
        showmeans=True,
        color=BAR_COLORS[0][0]
    )

    # Add background color for different groups
    for i, col in enumerate(ratio):
        style = col.split("_")[1]
        faceColor = (
            BAR_COLORS[1][0] if style in {"children", "young", "middle", "elderly", "1Num", "2Num", "3Num"} else 
            BAR_COLORS[1][1] if style in {"Male", "Female", "POIAll"} else
            BAR_COLORS[1][2] if style == "All" else
            BAR_COLORS[1][3] if style == "2024" else
            BAR_COLORS[1][4]
        )
        ax3.axvspan(
            i - 0.5,
            i + 0.5,
            facecolor=faceColor,
            alpha=0.2,
            zorder=0
        )

    # Add significance annotations
    ax3.set_xlim(-0.5, len(ratio) - 0.5)
    ymin, ymax = ax3.get_ylim()
    startHeight = ymax
    lineStep = 0.12 * (ymax - ymin)
    maxHeight = startHeight
    
    pairs = {startHeight: (0,0)}
    for x1, group1 in enumerate(ratio[:-1]):
        for x2, group2 in enumerate(ratio[x1+1:]):
            g1Name = group1.split("_")[1]
            g2Name = group2.split("_")[1]
            if (
                (g1Name in {"children", "young", "middle", "elderly"} and g2Name in {"Male", "Female", "2024"}) or
                (g1Name in {"Male", "Female"} and g2Name == "2024")
            ):
                continue
            
            x2 += (x1 + 1)

            lineHeight = startHeight
            pairMin, pairMax = pairs[lineHeight]
            for lineHeight in pairs.keys():
                pairMin, pairMax = pairs[lineHeight]
                if (x1 >= pairMin and x1 < pairMax) or (x2 > pairMin and x2 <= pairMax):
                    lineHeight += lineStep
                else:
                    break

            row = wilcoxon[(wilcoxon["group1"] == group1) & (wilcoxon["group2"] == group2)]
            
            ## Draw line
            ax3.plot(
                [x1, x1, x2, x2], 
                [
                    lineHeight,
                    lineHeight + lineStep/3, 
                    lineHeight + lineStep/3,
                    lineHeight
                ],
                color="#000000",
                lw=1.5
            )
            
            ## Add text
            m = row["magnitude"].values[0]
            e = row["effsize"].values[0]
            s = row["significance"].values[0]
            text = f"{e:.4f} ({m})" if m != "negligible" else f"{e:.4f}"
            if s not in {'', '.'}:
                text = f"{text}$^{{{s}}}$"
            ax3.text(
                (x1 + x2) / 2,
                lineHeight + lineStep/2,
                text,
                ha="center", va="bottom",
                fontsize=NOTE_SIZE,
                bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8)
            )
            
            ## Save line info
            if lineHeight not in pairs: pairs[lineHeight] = (x1, x2)
            else: pairs[lineHeight] = (min(x1, pairMin), max(x2, pairMax))
            maxHeight = max(maxHeight, lineHeight)
    
    ax3.set_ylim(
        ymin,
        max(2, maxHeight + lineStep) if accOrEquity == "accessibility" else max(0.8, maxHeight + lineStep)
    )
    # ax3.yaxis.tick_right()
    ax3.set_ylabel("Change ratio (%)")
    plt.standAxisName(ax3, 'x', STAND_NAME)

    plt.plot(os.path.join(savePath, f"{scale}_{accOrEquity}.jpg"))

    return

# Debug
if __name__ == "__main__":
    root = r"C:\\0_PolyU\\test"
    ANALY_RESULT = os.path.join(root, "3km")
    CITY_RESULT = os.path.join(ANALY_RESULT, "city.csv")
    iso3 = os.path.join(root, "iso3.csv")
    # popAndPOIAnalysis(CITY_RESULT, "popStatic", ANALY_RESULT)
    # popAndPOIAnalysis(CITY_RESULT, "popDynamic", ANALY_RESULT)
    popAndPOIAnalysis(CITY_RESULT, "pop", ANALY_RESULT)
    popAndPOIAnalysis(CITY_RESULT, "POI", ANALY_RESULT)
    # popAndPOIAnalysis(iso3, "popStatic", root)
    # popAndPOIAnalysis(iso3, "popDynamic", root)
    # popAndPOIAnalysis(iso3, "POI", root)

    popAndPOIAnalysis(CITY_RESULT, "popStatic", ANALY_RESULT, accOrEquity="equity")
    popAndPOIAnalysis(CITY_RESULT, "popDynamic", ANALY_RESULT, accOrEquity="equity")
    popAndPOIAnalysis(CITY_RESULT, "POI", ANALY_RESULT, accOrEquity="equity")
    # popAndPOIAnalysis(iso3, "popStatic", root, accOrEquity="equity")
    # popAndPOIAnalysis(iso3, "popDynamic", root, accOrEquity="equity")
    # popAndPOIAnalysis(iso3, "POI", root, accOrEquity="equity")