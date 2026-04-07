import sys, os
import numpy as np
import seaborn as sns
import matplotlib.lines as mlines
from pandas import DataFrame

sys.path.append(".") # Set path to the roots

from _plot import plt, BAR_COLORS, NOTE_SIZE, BOX_KWARGS
from analysis.__readNode import readNode
from analysis.__setting import STAND_NAME, AColumns
from analysis.__statisticalDiff import Wilcoxon
from analysis.__calRatio import calRatio

MEAN_LEGEND = mlines.Line2D(
    [], [], 
    marker='^', color="green",
    linestyle="None",
    markersize=10,
    label="Mean"
)

def popAndPOIAnalysis(
    path: str, analysisType: str,
    savePath: str,
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
    A_BEFORE, A_AFTER = AColumns(analysisType, accOrEquity, 0)

    df, _, _ = readNode(path, minEvcsNum)
    scale = os.path.basename(path).split('.')[0]
    addCol = ["iso3", "city"] if scale == "city" else ["iso3"]
    df = df[A_BEFORE + A_AFTER + addCol]
    savePath = os.path.join(savePath, analysisType)
    if not os.path.exists(savePath): os.makedirs(savePath)

    results = calRatio(
        df, A_BEFORE, A_AFTER, True, True if accOrEquity == "equity" else False
    )
    
    # Save change ratio
    print("\n")
    
    # Equity
    if accOrEquity == "accessibility":
        __accessibilityAnalysis(*results, scale, savePath)
        __equityAnalysis(*results, analysisType, scale, savePath, forAcc=True,)
    else:
        __equityAnalysis(*results, analysisType, scale, savePath)

    return

def __accessibilityAnalysis(
    df: DataFrame, ratio: np.ndarray, zeroCounts: np.ndarray, nonZeroCounts: np.ndarray,
    scale: str,
    savePath: str
) -> None:
    # Plot
    subplot = plt.subplot("D", 1, 2, widthRatios=[12, 1])
    ax1 = subplot.axs[0] # Non zero ECDF
    ax2 = subplot.axs[1] # Relation scatter
    legends = []

    # addLegend = relationAnalysis(
    #     path,
    #     "popStatic" if analysisType == "pop" else analysisType,
    #     os.path.dirname(savePath)
    # ).plot((accOrEquity, "evcs"), ax=ax1)
    # legends.extend(addLegend)

    ## non-0 ECDF
    group: str
    i = 0
    for group in ratio:
        # Only draw final results for accessibility
        if "All" not in group:
            i += 1
            continue
        groupData = df[group][(df[group] != 0) & df[group].notna()]
        if len(groupData) > 0:
            # ECDF
            x = np.sort(groupData)
            y = np.arange(1, len(x)+1) / len(x)
            ax1.plot(
                x, y,
                # label=STAND_NAME.get(group, group).capitalize().replace("\n", " ").replace("poi", "POI"),
                color=BAR_COLORS[0][0],
                linewidth=5
            )
            # Mean and median
            mean = float(np.mean(groupData))
            p50 = float(np.median(groupData))
            ax1.axvline(mean, color="red", linestyle='--', linewidth=3, label=f'Mean: {mean:.2f}')
            ax1.axvline(p50, color="orange", linestyle=':', linewidth=3, alpha=0.8, label=f'Median: {p50:.2f}')
        break
    
    ax1.legend()
    ax1.set_xlabel("Change ratio (%)")
    ax1.set_ylabel("Cumulative probability")
    # ax1.yaxis.tick_right()
    # ax1.yaxis.set_label_position("right")
    
    # Proportion of 0 bar
    __portionBar(ax2, [ratio[i]], [zeroCounts[i]], [nonZeroCounts[i]], horizontal=False)
    ## Adjust labels
    ax2.yaxis.set_ticks_position("right")
    ax2.yaxis.set_label_position("right")

    # Add legends for portation bar
    legends.extend(ax2.get_legend_handles_labels()[0])

    subplot.fig.legend(
        loc="lower center",
        bbox_to_anchor=(0.5, 0.07),
        handles=legends,
        ncol=6
    )

    plt.plot(savePath, f"{scale}_accessibility.jpg")

    return

def __equityAnalysis(
    df: DataFrame, ratio: np.ndarray, zeroCounts: np.ndarray, nonZeroCounts: np.ndarray,
    analysisType: str, scale: str,
    savePath: str, forAcc: bool = False,
) -> None:
    title = "accessibility" if forAcc else "equity"
    legends = []

    # Plot
    if forAcc:
        fig, ax2 = plt.figure("D")
    else:
        subplot = plt.subplot("H31", 2, 1, heightRatios=[2, 3])
        ax1 = subplot.axs[0] # Proportion of zero bar
        ax2 = subplot.axs[1] # Box plot
        fig = subplot.fig
    
        # Proportion of 0 bar
        __portionBar(ax1, ratio, zeroCounts, nonZeroCounts)

        # Add legends for portation bar
        legends.extend(ax1.get_legend_handles_labels()[0])

    # Wilcoxon
    wilcoxon = Wilcoxon(
        ratio,
        df,
        f"{analysisType} {title}",
        os.path.join(savePath, f"{scale}_{title}_wilcoxon.csv") if savePath != "" else ""
    )

    # Box plot
    sns.boxplot(
        data=df[ratio],
        ax=ax2,
        showfliers=False,
        showmeans=True,
        color=BAR_COLORS[0][0],
        **BOX_KWARGS
    )

    ## Add legends for triangle mean point
    legends.append(MEAN_LEGEND)

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
        ax2.axvspan(
            i - 0.5,
            i + 0.5,
            facecolor=faceColor,
            alpha=0.2,
            zorder=0
        )

    # Add significance annotations
    ax2.set_xlim(-0.5, len(ratio) - 0.5)
    ymin, ymax = ax2.get_ylim()
    startHeight = ymax
    lineStep = 0.15 * (ymax - ymin)
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
            ax2.plot(
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
            ax2.text(
                (x1 + x2) / 2,
                lineHeight + lineStep/2,
                text,
                ha="center", va="bottom",
                fontsize=NOTE_SIZE*0.8,
                bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8)
            )
            
            ## Save line info
            if lineHeight not in pairs: pairs[lineHeight] = (x1, x2)
            else: pairs[lineHeight] = (min(x1, pairMin), max(x2, pairMax))
            maxHeight = max(maxHeight, lineHeight)
    
    ax2.set_ylim(
        ymin,
        max(0.8, maxHeight + lineStep)
    )
    # ax2.yaxis.tick_right()
    ax2.set_ylabel("Change ratio (%)")
    plt.standAxisName(ax2, 'x', STAND_NAME)

    if not forAcc:
        fig.legend(
            loc="lower center",
            bbox_to_anchor=(0.5, -0.008),
            handles=legends,
            ncol=6
        )

    plt.plot(savePath, f"{scale}_{title}.jpg")

    return

# Proportion of 0 bar
def __portionBar(
    ax: plt.Axes,
    ratio: np.ndarray | list, zeroCounts: np.ndarray | list, nonZeroCounts: np.ndarray | list,
    horizontal: bool = True
) -> None:
    yPos = np.arange(len(ratio))
    
    totalCounts = [zero + non_zero for zero, non_zero in zip(zeroCounts, nonZeroCounts)]
    zeroPercent = [zero / total * 100 for zero, total in zip(zeroCounts, totalCounts)]
    nonZeroPercent = [non_zero / total * 100 for non_zero, total in zip(nonZeroCounts, totalCounts)]
    
    ## Stacked horizontal bar
    if horizontal:
        ax.barh(yPos, zeroPercent, height=0.6, label="Zero values", alpha=0.7)
        ax.barh(yPos, nonZeroPercent, height=0.6, left=zeroPercent, label="Non-zero values", alpha=0.7)
        ax.set_yticks(yPos)
        ax.set_yticklabels(ratio)
        ax.set_xlabel("Percentage (%)")
    else:
        ax.bar(yPos, zeroPercent, width=0.6, label="Zero values", alpha=0.7)
        ax.bar(yPos, nonZeroPercent, width=0.6, bottom=zeroPercent, label="Non-zero values", alpha=0.7)
        ax.set_xticks(yPos)
        ax.set_xticklabels(ratio)
        ax.set_ylabel("Percentage (%)")
    
    ## Label
    for i, (zp, nzp) in enumerate(zip(zeroPercent, nonZeroPercent)):
        ax.text(
            zp/2 if horizontal else i,
            i if horizontal else zp/2,
            f"{zp:.2f}%",
            ha="center", va="center",
            fontsize=NOTE_SIZE, fontweight="bold",
            rotation=0 if horizontal else 90
        ) if zp != 0 else None
        ax.text(
            zp + nzp/2 if horizontal else i,
            i if horizontal else zp + nzp/2,
            f"{nzp:.2f}%",
            ha="center", va="center",
            fontsize=NOTE_SIZE, fontweight="bold",
            rotation=0 if horizontal else 90
        )

    ax.grid(True, alpha=0.3, axis='x')
    plt.standAxisName(ax, 'y' if horizontal else 'x', STAND_NAME)

    return

# Debug
if __name__ == "__main__":
    root = r"C:\\0_PolyU\\test"
    ANALY_RESULT = os.path.join(root, "3km")
    CITY_RESULT = os.path.join(ANALY_RESULT, "city.csv")

    popAndPOIAnalysis(CITY_RESULT, "pop", ANALY_RESULT)
    popAndPOIAnalysis(CITY_RESULT, "POI", ANALY_RESULT)

    popAndPOIAnalysis(CITY_RESULT, "pop", ANALY_RESULT, accOrEquity="equity")
    popAndPOIAnalysis(CITY_RESULT, "POI", ANALY_RESULT, accOrEquity="equity")