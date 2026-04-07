import sys, warnings
import pandas as pd
import geopandas as gpd
import seaborn as sns
from scipy import stats

sys.path.append(".") # Set path to the roots
warnings.filterwarnings("ignore", category=UserWarning)

from _plot import plt, BAR_COLORS, TICK_SIZE, BOX_KWARGS, NOTE_SIZE
from _function.readChangeRatio import readChangeRatio

# __STAND_NAME = [
#     "EVCS density (count/km²)", "EVCS coverage (%)", "Road density (km/km²)", "Road coverage (%)", "Road connectivity"
# ]
# __INDICATOR_COLS = [
#     "EVCSDensity", "EVCScoverage", "roadDensity", "roadCoverage", "roadConnectivity"
# ]

def __sample(df: pd.DataFrame) -> pd.DataFrame:
    def __quantileSample(group: pd.DataFrame) -> pd.DataFrame:
        import numpy as np
        MAX_POINTS_PER_GROUP = 200
        N_QUANTILES = 10

        if len(group) <= MAX_POINTS_PER_GROUP:
            return group
        # Calculate at which quantile level each point falls
        qs = np.linspace(0, 1, N_QUANTILES + 1)
        bins = group["value"].quantile(qs).values
        # Eliminate duplicate bin boundaries (there may be a large number of identical values)
        bins = np.unique(np.asarray(bins))
        group["bin"] = np.digitize(group["value"], bins, right=True)

        n = max(1, MAX_POINTS_PER_GROUP // len(np.unique(group["bin"])))
        
        return group.groupby("bin", group_keys=False)[["indicator", "value"]].apply(
            lambda g: g.sample(
                n=min(len(g), n), random_state=0
            )
        )
    
    sample = df.reset_index().melt(
        id_vars=[df.index.name or "index"],
        var_name="indicator",
        value_name="value"
    )
    
    return sample.groupby("indicator", group_keys=False)[["indicator", "value"]].apply(__quantileSample)

def EVCSAndRoadAnalysis(path: str, figsize: str = "W32") -> None: # , indicator: str
    df, COL = readChangeRatio(path, initial=True)
    # indicatorDf = gpd.read_file(indicator, layer="city", encoding="utf-8").set_index("city")[
    #     __INDICATOR_COLS
    # ]
    savePath = os.path.dirname(path)

    # plotsName = []
    # plotDfs = [[] for _ in range(5)]
    # plotSample = [[] for _ in range(5)]

    for i, name in enumerate(("Population-based", "Facility-based")):
        colEVCS = COL.EVCS[i]
        colRoad = COL.road[i]
        colAll = COL.all[i]
        df["collaboration"] = df[colAll] - df[colEVCS] - df[colRoad]
        df["diff"] = df[colEVCS].abs() - df[colRoad].abs()

        a_wins = (df["diff"] > 0).sum()
        b_wins = (df["diff"] < 0).sum()
        total = df.shape[0]

        print(f"{name}:")

        plots = plt.subplot(figsize, 1, 2, widthRatios=[12, 1], legend=False)
        axs = plots.axs

        # Sactter: x-EVCS, y-Road, use color to show collaboration
        scatter = axs[0].scatter(
            x=df[colEVCS], y=df[colRoad], 
            c=df["collaboration"], cmap="RdYlBu", 
            alpha=0.7,
            edgecolors="gray",
            s=80
        )
        ## y=x
        axs[0].axline((0, 0), slope=1, linestyle='--', color="red", alpha=0.5)
        ## Colorbar for collaboration
        cbar =plt.plt.colorbar(scatter, ax=axs[0], label="Collaboration")
        ## Labels
        axs[0].set_xlabel("EVCS")
        axs[0].set_ylabel("Road")
        bbox = dict(boxstyle='round', facecolor='white', alpha=0.7)
        axs[0].text(
            0.05, 0.95,
            f"EVCS > Road: {a_wins/total:.2%}",
            transform=axs[0].transAxes,
            fontsize=NOTE_SIZE, va='top',
            bbox=bbox
        )
        axs[0].text(
            0.95, 0.05,
            f"Road > EVCS: {b_wins/total:.2%}",
            transform=axs[0].transAxes,
            fontsize=NOTE_SIZE, ha='right', va='bottom',
            bbox=bbox
        )

        # Boxplot for collaboration
        sns.swarmplot(
            data=__sample(df[["collaboration"]]), x="indicator", y="value",
            ax=axs[1],
            color="gray",
            alpha=0.5,
            size=3,
            zorder=1
        )
        axs[1].boxplot(
            df["collaboration"],
            positions=[0],
            patch_artist=True, showfliers=False,
            boxprops={
                "facecolor": "gray",
                "edgecolor": "gray"
            },
            medianprops = {"color": "white"},
            whiskerprops = {"color": "gray"},
            capprops = {"color": "gray"},
            zorder=2
        )
        axs[1].set_xlim(-0.2, 0.2)
        axs[1].set_xticklabels([])
        axs[1].set_xlabel("")
        axs[1].set_ylabel("")
        ## Add label for collaboration boxplot, positioned between the box and colorbar
        bboxColorBar  = cbar.ax.get_position()
        bboxBoxPlot = axs[1].get_position()
        centerX = (bboxColorBar.x0 + bboxBoxPlot.x1) / 2 + 0.06
        bottomY = min(bboxColorBar.y0, bboxBoxPlot.y0) - 0.03
        plots.fig.text(
            centerX, bottomY,
            "Collaboration",
            ha="center", va="top",
            fontdict={"size": TICK_SIZE}
        )
        ## Adjust y-axis of boxplot to match colorbar
        axs[1].set_ylim(cbar.norm.vmin, cbar.norm.vmax)
        axs[1].set_yticks(cbar.get_ticks())
        axs[1].yaxis.set_ticks_position("right")
        axs[1].yaxis.set_label_position("right")
        ## Delete ticks and label of colorbar
        cbar.ax.yaxis.set_ticks([])
        cbar.ax.set_ylabel("")

        plt.plot(savePath, f"{"dissertation" if figsize == "DS" else "fig3"}_{name}_collaboration.jpg")

        # Analysis metrics
        _, p = stats.ttest_rel(df[colEVCS], df[colRoad])

        metrics_text = f"""
            Analysis Result:
            ────────────────
            City numbers: {len(df):d}
            ────────────────
            Average decrease value:
            • Only EVCS: {df[colEVCS].mean():.2f}
            • Only roads: {df[colRoad].mean():.2f}
            • Both EVCS and roads: {df[colAll].mean():.2f}
            • Collaboration effects: {df["collaboration"].mean():.2f}
            ────────────────
            Comparison between the values of decreasing in accessibility:
            • EVCS > Roads: {a_wins:d} ({a_wins/len(df)*100:.1f}%)
            • Roads > EVCS: {b_wins:d} ({b_wins/len(df)*100:.1f}%)
            ────────────────
            Statistic significance:
            • P-value of paried t-test: {p:.4f}
            • {"Roads have more influence than EVCS" if df["diff"].mean() < 0 else "EVCS have more influence than Roads"}
            """
        print(metrics_text)

    #     # Split by dominant effect
    #     ## EVCS dominant cities
    #     evcsDominant = indicatorDf[indicatorDf.index.isin(df[df["diff"] > 0]['city'].unique())]
    #     print("EVCS dominant cities mean:\n", evcsDominant.mean())
    #     ## Roads dominant cities
    #     roadDominant = indicatorDf[indicatorDf.index.isin(df[df["diff"] < 0]['city'].unique())]
    #     print("Road dominant cities mean:\n", roadDominant.mean())

    #     # Prepare data for boxplot
    #     for name2, data in zip(["EVCS dominant", "road dominant"], [evcsDominant, roadDominant]):
    #         plotName = f"{name} and {name2}"
    #         plotsName.append(plotName)
    #         sampleDf = __sample(data)
    #         for n, col in enumerate(__INDICATOR_COLS):
    #             plotDfs[n].append(pd.DataFrame(
    #                 {"value": data[col], "indicator": plotName}
    #             ))
    #             plotSample[n].append(pd.DataFrame({
    #                 "value": sampleDf.loc[sampleDf["indicator"] == col, "value"],
    #                 "indicator": plotName
    #             }))

    # # Box plot for different indicator
    # indicatorPlots = plt.subplot("W", 1, 5)
    # indicatorAxs = indicatorPlots.axs
    # palette = {x: y for x,y in zip(plotsName, BAR_COLORS[0])}
    # for n, col in enumerate(__INDICATOR_COLS):
    #     ax = indicatorAxs[n]

    #     sns.swarmplot(
    #         data=pd.concat(plotSample[n]), x="indicator", y="value",
    #         ax=ax,
    #         color="gray",
    #         alpha=0.5,
    #         size=3,
    #         zorder=1,
    #         order=plotsName # Fix indicator columns order
    #     )
        
    #     data = pd.concat(plotDfs[n])
    #     sns.boxplot(
    #         data=data, x="indicator", y="value",
    #         ax=ax,
    #         width=0.5,
    #         patch_artist=True, showmeans=True, showfliers=False,
    #         legend=True if n == 0 else False,
    #         hue="indicator",
    #         palette=palette,  # Custom color list
    #         boxprops={"edgecolor": "gray", "alpha": 0.8},
    #         zorder=2,
    #         order=plotsName, # Fix indicator columns order
    #         **BOX_KWARGS
    #     )

    #     # Adjust y max lim
    #     whiskerRanges = 0
    #     for name, group in data.groupby("indicator"):
    #         q1 = group["value"].quantile(0.25)
    #         q3 = group["value"].quantile(0.75)
    #         iqr = q3 - q1
    #         upperWhisker = group["value"][group["value"] <= q3 + 1.5 * iqr].max()
    #         whiskerRanges = max(whiskerRanges, upperWhisker)

    #     ax.set_ylim(ymin=0, ymax=whiskerRanges * 1.1)
        
    #     if n == 0:
    #         ax.set(xlabel=__STAND_NAME[n], ylabel="Indicator value")
    #     else:
    #         ax.set(xlabel=__STAND_NAME[n], ylabel="")
    #     ax.set_xticklabels([])

    # # Add legends
    # indicatorPlots.legend(ncols=2)
    # plt.plot(savePath, "fig3_indicator.jpg")

    return

if __name__ == "__main__":
    import os
    ANALY_RESULT = r"C:\0_PolyU\test\3km"
    INDICATOR = r"C:\\0_PolyU\\test\\indicator.gpkg"
    EVCSAndRoadAnalysis(os.path.join(ANALY_RESULT, "changeRatio_result.csv")) #, INDICATOR

    # For dissertation
    EVCSAndRoadAnalysis(os.path.join(ANALY_RESULT, "changeRatio_result.csv"), "DS") #, INDICATOR