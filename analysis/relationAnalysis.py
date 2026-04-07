import sys, os
import pandas as pd
import numpy as np
from scipy import stats
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
from matplotlib.pyplot import scatter

sys.path.append(".") # Set path to the roots

from _plot import plt, BAR_COLORS, NOTE_SIZE
from analysis.__readNode import readNode
from analysis.__setting import AColumns
from analysis.__calRatio import calRatio

_STAND_NAME = {
    "A_All_change": "population-based resilience",
    "A_2024_change": "resilience",
    "A_POIAll_change": "facility-based resilience",
    "A_All_Gini_change": "Gini coefficient",
    "A_2024_Gini_change": "Gini coefficient",
    "A_POIAll_Gini_change": "Gini coefficient"
}

class relationAnalysis:
    __slots__ = ["df", "relationDict", "continent", "scale", "savePath", "continentColors"]

    def __init__(self, path: str, analysisType: str, savePath: str, minEvcsNum: int = 0) -> None:
        df, _, _ = readNode(path, minEvcsNum)
        df["EVCS_Change"] = (df["EVCSNum_After"] / df["EVCSNum"] - 1) * 100

        A_ACC_BEFORE, A_ACC_AFTER = AColumns(analysisType, "accessibility")
        A_EQU_BEFORE, A_EQU_AFTER = AColumns(analysisType, "equity")
        # Only consider the overall demograpic/POI
        A_BEFORE = [A_ACC_BEFORE[-1], A_EQU_BEFORE[-1]]
        A_AFTER = [A_ACC_AFTER[-1], A_EQU_AFTER[-1]]
        self.scale = os.path.basename(path).split('.')[0]
        addCol = ["iso3", "city", "EVCSNum", "EVCS_Change"] if self.scale == "city" else ["iso3", "EVCSNum"]
        df = df[A_BEFORE + A_AFTER + addCol]
        self.savePath = os.path.join(savePath, "pop" if analysisType == "popStatic" else "POI")
        if not os.path.exists(self.savePath): os.makedirs(self.savePath)

        df, ratio = calRatio(df, A_BEFORE, A_AFTER)
        df: pd.DataFrame = df[addCol + ratio.tolist()]

        continent = pd.read_csv(r"analysis\dcw-countries.csv", usecols=["iso3", "continent"], index_col="iso3",)
        self.df: pd.DataFrame = df.join(continent, on="iso3")
        self.df.dropna(inplace=True)
        self.df = self.df[(self.df[ratio[0]] > -99) & (self.df[ratio[1]] > -99)]
        self.relationDict = {
            "accessibility": ratio[0],
            "equity": ratio[1],
            "evcs": "EVCS_Change",
            "roads": "roadsLengthChange"
        }
        self.continent = self.df["continent"].unique()
        del df

        # Assign colors based on continent
        self.continentColors = {
            x: y for x, y in zip(self.continent, BAR_COLORS[2][0:-1] + BAR_COLORS[3])
        }
        self.df["color"] = self.df["continent"].map(self.continentColors)

        return
    
    # @staticmethod
    # def __identifyOutliers(df: pd.DataFrame, x: str, y: str, threshold: float) -> None:
    #     """
    #     Recognize outlier cities based on Z-score method.
    #     Default threshold is 2 standard deviations.
    #     """
    #     # z-score outlier
    #     df["z1"] = np.abs(stats.zscore(df[x]))
    #     df['z2'] = np.abs(stats.zscore(df[y]))
    #     df["outlier"] = (df["z1"] > threshold) | (df["z2"] > threshold)
        
    #     # Calculate Euclidean distance from origin
    #     df["d"] = np.sqrt(
    #         df[x]**2 + df[y]**2
    #     )
    #     distanceThreshold = np.percentile(df["d"], 90)
    #     df["extreme"] = df["d"] > distanceThreshold
        
    #     return
    
    def __outputInformation(self, x: str, y: str, dfIn: pd.DataFrame | None = None) -> tuple[str, float]:
        df = self.df if dfIn is None else dfIn
        # print("\n=== Outlier cities ===")
        # outliers = df[df["outlier"]]
        # print(f"The number of outlire cities is {outliers.shape[0]}")
        # print(outliers.to_string(index=False))

        result = "=== Statistic information by continents ===\n"
        summary = df.groupby("continent").agg({
            x: ["mean", "std", "min", "max"],
            y: ["mean", "std", "min", "max"],
            self.scale: "count"
        }).round(2)
        summary.columns = [
            " ".join(
                [str(_STAND_NAME.get(x, x)) for x in col if x not in {None, "", " "}]
            ) for col in summary.columns
        ]
        result += summary.to_string(justify="center")

        # correlation coefficient
        correlation = df[x].corr(df[y])
        result += f"\n\nThe correlation coefficient between {_STAND_NAME.get(x)} and {_STAND_NAME.get(y)} is: {correlation:.4f}"

        # correlation coefficient by continents
        result += "\n\n=== correlation coefficient by continents ==="
        for continent in self.continent:
            subset = df[df["continent"] == continent]
            if len(subset) > 1:
                corr = subset[x].corr(subset[y])
                result += f"\n{continent}: {corr:.4f} (Total: {len(subset)})"

        result += "\n\n"

        return result, correlation

    def __drawLine(self, ax: plt.Axes, x: str, y: str, corr: float, dfIn: pd.DataFrame | None = None) -> None:
        df = self.df if dfIn is None else dfIn.dropna(subset=[x, y])

        ax.axhline(y=0, color="gray", linestyle="--", linewidth=0.8, alpha=0.7)
        ax.axvline(x=0, color="gray", linestyle="--", linewidth=0.8, alpha=0.7)
        
        # Add trend line
        z = np.polyfit(df[x], df[y], 1)
        p = np.poly1d(z)
        xTrend = np.linspace(df[x].min(), df[x].max(), 100)
        ax.plot(xTrend, p(xTrend), color="black", linewidth=2, linestyle='-', alpha=0.7, label="Overall Trend Line")
        
        # # Label outlier cities
        # self.__identifyOutliers(df, x, y, 4)
        # outliers = df[df["outlier"]]
        # for _, row in outliers.iterrows():
        #     ax.annotate(
        #         row[self.scale],
        #         xy=(row[x], row[y]),
        #         xytext=(5, 5),
        #         textcoords="offset points",
        #         fontsize=NOTE_SIZE,
        #         fontweight="bold",
        #         bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
        #         arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.2', color='red', alpha=0.6)
        #     )

        # R2
        yPred = p(df[x])
        yMean = np.mean(df[y])
        res = np.sum((df[y] - yPred) ** 2)
        tot = np.sum((df[y] - yMean) ** 2) # type: ignore
        r2: pd.Series = 1 - res / tot
        ## Add text
        ax.text(
            0.95, 0.15 if corr > 0 else 0.95,
            f"R² = {r2:.4f}\nCorrelation = {corr:.4f}", 
            transform=ax.transAxes,
            fontsize=NOTE_SIZE,
            verticalalignment="top",
            horizontalalignment="right",
            bbox=dict(boxstyle="round", facecolor="#FFFFFF", alpha=0.8)
        )

        return
    
    def legends(self, byContinent: bool) -> list:
        legends = []

        if byContinent:
            # Continent
            legends.extend([
                Patch(color=color, label=continent) 
                for continent, color in self.continentColors.items()
            ])

            # EVCS
            smin = int(self.df["EVCSNum"].min())
            smax = int(self.df["EVCSNum"].max() / 100) * 100
            smid = int(((smin + smax) >> 1) / 100) * 100
            sizes = [smin, smid, smax]

            for size in sizes:
                legends.append(
                    scatter([], [], s=size/10, c="gray",
                            linewidth=1, label=f"EVCS: {size}")
                )

        # trends
        legends.append(
            Line2D([], [], color="black", linewidth=2, 
                linestyle="-", label="Trend Line")
        )

        return legends
    
    def plot(
        self,
        relation: tuple[str, str] = ("accessibility", "evcs"),
        indicators: pd.DataFrame | None = None,
        byContinent: bool = True,
        standName: dict[str, str] | None = None,
        figsize: str = "W", color: int = 0, ax: plt.Axes | None = None,
        saveFig: bool = False
    ) -> list:
        if ax is None or saveFig: fig, ax = plt.figure(figsize)
        STAND_NAME = _STAND_NAME if standName is None else standName
        
        # Plot, the point size is based on EVCS number
        result = ""
        x, y = self.relationDict[relation[0]], self.relationDict[relation[1]]

        # Add other indicators
        df = self.df if indicators is None else self.df.join(
            indicators.set_index("city")[[x if x in indicators.columns else y]], on="city"
        )

        rstr, corr = self.__outputInformation(x, y, df)
        result += f"Relation ship between {STAND_NAME.get(x)} and {STAND_NAME.get(y)}:\n{rstr}"

        for continent in self.continent:
            subset = df[df["continent"] == continent]
            ax.scatter(
                x=subset[x],
                y=subset[y],
                s=subset["EVCSNum"] / 10 if byContinent else None, # Point size (Million)
                c=self.continentColors[continent] if byContinent else BAR_COLORS[0][color],
                linewidth=1,
                label=continent if byContinent else None
            )
        
        self.__drawLine(ax, x, y, corr, df)
        ax.set_xlabel("Change in {} (%)".format(STAND_NAME.get(x, x)))
        ax.set_ylabel("Change in {} (%)".format(STAND_NAME.get(y, y)))

        legend = self.legends(byContinent)
        if saveFig:
            # fig.legend(
            #     handles=legend,
            #     loc="lower center",
            #     ncol=len(self.continent) + 1,
            #     frameon=True,
            #     fancybox=True,
            #     borderpad=1,
            #     labelspacing=0.5,
            #     handletextpad=1,
            #     columnspacing=1.5
            # )
            plt.plot(self.savePath, f"fig2_{self.scale}_{x}&{y}.jpg")

        # with open(os.path.join(self.savePath, f"{self.scale}_{x}&{y}.txt"), 'w') as f:
        #     f.write(result)

        return legend

if __name__ == "__main__":
    root = r"C:\\0_PolyU\\test"
    ANALY_RESULT = os.path.join(root, "3km")
    CITY_RESULT = os.path.join(ANALY_RESULT, "city.csv")
    INDICATOR = os.path.join(root, "indicator.gpkg")

    # fig 2: relationship between accessibility and equity
    pop = relationAnalysis(CITY_RESULT, "popStatic", ANALY_RESULT)
    poi = relationAnalysis(CITY_RESULT, "POI", ANALY_RESULT)
    pop.plot(("accessibility", "equity"), byContinent=False, figsize="D", saveFig=True)
    poi.relationDict["poiAcc"] = pop.relationDict["accessibility"]
    poi.plot(("poiAcc", "accessibility"), indicators=pop.df[["city", "A_All_change"]], byContinent=False, figsize="D", color=3, saveFig=True)

    # ## For dissertaion
    # from _plot import plt
    # figures = plt.subplot("W", 1, 2, legend=True, sharey=True)
    # axs = figures.axs
    # legends = pop.plot(("accessibility", "equity"), ax=axs[0])
    # poi.plot(("accessibility", "equity"), ax=axs[1])
    # axs[1].set_ylabel("")

    # figures.fig.legend(
    #     handles=legends,
    #     loc="lower center",
    #     ncol=5,
    #     frameon=True,
    #     fancybox=True,
    #     borderpad=1,
    #     labelspacing=0.5,
    #     handletextpad=1,
    #     columnspacing=1.5
    # )
    # plt.plot(ANALY_RESULT, "dissertation_city_accessibility&equity.jpg", fig=figures.fig)

    # fig 3: relationship between accessibility and EVCS change
    from _plot import plt
    import geopandas as gpd

    standName = {
        "A_All_change": "resilience",
        "EVCS_Change": "EVCS count",
        "roadsLengthChange": "road length"
    }

    figures = plt.subplot("W31", 2, 1, legend=False)
    axs = figures.axs
    legends = relationAnalysis(CITY_RESULT, "popStatic", ANALY_RESULT).plot(("evcs", "accessibility"), standName=standName, ax=axs[0], byContinent=False)

    indicator = gpd.read_file(INDICATOR, layer="city")
    relationAnalysis(CITY_RESULT, "popStatic", ANALY_RESULT).plot(("roads", "accessibility"), standName=standName, ax=axs[1], color=2, indicators=indicator, byContinent=False)
    
    plt.plot(ANALY_RESULT, "fig3_city_accessibility&EVCS&Road.jpg")

    # # For dissertation
    # figures = plt.subplot("WN32", 1, 4, legend=True, sharey=True)
    # axs = figures.axs
    # legends = relationAnalysis(CITY_RESULT, "popStatic", ANALY_RESULT).plot(("evcs", "accessibility"), ax=axs[0], byContinent=False)
    # relationAnalysis(CITY_RESULT, "POI", ANALY_RESULT).plot(("evcs", "accessibility"), ax=axs[1], byContinent=False)

    # # Accessibility and roads length change
    # indicator = gpd.read_file(INDICATOR, layer="city")
    # relationAnalysis(CITY_RESULT, "popStatic", ANALY_RESULT).plot(("roads", "accessibility"), ax=axs[2], indicators=indicator, byContinent=False)
    # relationAnalysis(CITY_RESULT, "POI", ANALY_RESULT).plot(("roads", "accessibility"), ax=axs[3], indicators=indicator, byContinent=False)
    
    # figures.fig.legend(
    #     handles=legends,
    #     loc="lower center",
    #     ncol=5,
    #     frameon=True,
    #     fancybox=True,
    #     borderpad=1,
    #     labelspacing=0.5,
    #     handletextpad=1,
    #     columnspacing=1.5
    # )
    
    # plt.plot(ANALY_RESULT, "dissertation_city_accessibility&EVCS&Road.jpg")