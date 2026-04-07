import sys, textwrap
import pandas as pd
import geopandas as gpd
import numpy as np
import seaborn as sns
import statsmodels.api as sm
from matplotlib.ticker import PercentFormatter
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

sys.path.append(".") # Set path to the roots

from _function.readChangeRatio import readChangeRatio
from _function.readFiles import mkdir
from _plot import plt, BAR_COLORS, NOTE_SIZE
from analysis.__statisticalDiff import significanceStars

class clustering:
    __slots__ = [
        "df", "COL", "cities", "clusterCols",
        "savePath"
    ]

    # __CLUST_INDICATOR_COLS = [
    #     "EVCSPop", "EVCSConnectivity"
    # ]
    
    __INDICATOR_COLS = [
        "EVCScoverage","EVCSAggregation", 
        "EVCSRoadDens", "EVCSConnectivity", 
        "EVCSPopCover", "EVCSPop"
    ]

    __STAND_NAME = {
        "A_All_change_all": "Pop.-based accessibility change ratio (%)",
        "A_POIAll_change_all": "Facility-based accessibility change ratio (%)",
        "EVCScoverage": "EVCS coverage (%)",
        "EVCSDensity": "EVCS Density",
        "EVCSAggregation": "EVCS aggregation (NNI)",
        "EVCSConnectivity": "EVCS connectivity",
        "EVCSRoadDens": "EVCS road density (%)",
        "EVCSPopCover": "EVCS-Pop. coverage",
        "EVCSPop": "EVCS-Pop. disparity",
        "roadDensity": "Road density (km/km²)",
        "floodingCoverage": "Flooding coverage (%)",
        "floodingDisparity": "Flooding-Pop. disparity"
    }

    def __init__(self, path: str, indicator: str, clusterCols: list[str], savePath: str) -> None:
        self.df, self.COL = readChangeRatio(path, indexCity=True, initial=True)
        indicatorDf = gpd.read_file(indicator, layer="city", encoding="utf-8").set_index("city")[
            list(self.__STAND_NAME.keys())[2:]
        ].dropna()
        self.df = self.df.join(indicatorDf).dropna()
        self.clusterCols = clusterCols

        self.cities = indicatorDf.index
        self.savePath = os.path.join(savePath, "clustering")
        mkdir(self.savePath)

        # # Correlation
        # from analysis.regression import autoCorrelation
        # autoCorrelation(self.df, self.__INDICATOR_COLS, [], True)
        del indicatorDf

        return

    def __compareK(
        self,
        Xscaled: np.ndarray,
        maxTry: int = 10, fileName: str = "appendix_compare_K.jpg"
    ) -> None:
        plots = plt.subplot("W", 1, 2, legend=False)
        axs = plots.axs

        sse = []
        sil_scores = []
        for k in range(1, maxTry):
            kmeans = KMeans(n_clusters=k, random_state=42)
            kmeans.fit(Xscaled)
            sse.append(kmeans.inertia_)

            if k == 1: continue
            labels = kmeans.fit_predict(Xscaled)
            sil_scores.append(silhouette_score(Xscaled, labels))


        axs[0].plot(range(1,maxTry), sse, marker='o')
        axs[0].set_xlabel('k')
        axs[0].set_ylabel("SSE")
            
        axs[1].plot(range(2,maxTry), sil_scores, marker='o')
        axs[1].set_xlabel('k')
        axs[1].set_ylabel("Silhouette Score")

        plt.plot(self.savePath, fileName)
        
        return

    def __kmeans(
        self,
        k: int, Xscaled: np.ndarray, cols: list[str],
        saveName: str, index: pd.Index | None = None,
        clusterNames: dict | None = None
    ) -> tuple[np.ndarray, pd.DataFrame]:
        kmeans = KMeans(n_clusters=k, random_state=42)
        labels = kmeans.fit_predict(Xscaled)
        df = pd.DataFrame(labels, columns=["cluster"], index=self.cities if index is None else index).join(self.df)
        clusters = df["cluster"].unique()
        clusters.sort()
        total = len(cols)

        # Only return result if only have one cluster col
        if total == 1:
            return clusters, df

        # Clusting results
        row = 1 if total < 6 else 2
        col = 1 if total < 3 else 2 if total < 5 else 3
        plots = plt.subplot("W" if col > 1 else "W32", row, col)
        for i in range(0, row*col):
            ax = plots.axs[i]
            x = cols[2*i if total%2==0 else i]
            y = cols[2*i+1 if total%2==0 else i+1] if total > 1 else "A_All"
            sns.scatterplot(
                data=df,
                x=x, y=y,
                ax=ax,
                hue="cluster", palette="Set1",
            )

            ax.set_xlabel(self.__STAND_NAME[x])
            ax.set_ylabel(self.__STAND_NAME[y])
        
        handles, labels = plots.axs[0].get_legend_handles_labels()
        labels = [f"Cluster {l}: {clusterNames[int(l)]}" for l in labels if l != "cluster"] if clusterNames is not None else labels
        plots.legend(handles=handles, labels=labels, ncol=1) # , title="Clusters"

        # Statistic
        table = []
        for c in clusters:
            sub: pd.DataFrame = df[df["cluster"] == c]
            table.append([
                f"{c}",
                f"{sub[cols[0]].mean():.2f}",
                f"{sub[cols[1]].mean():.2f}",
                f"{sub[cols[0]].median():.2f}",
                f"{sub[cols[1]].median():.2f}"
            ])
        print("="*30 + " Clustering Result Statistic" + "="*30)
        print(np.array(table))
        print("="*70)

        plt.plot(self.savePath, "{}.jpg".format(saveName))

        return clusters, df
    
    def run(self, k: int, saveName: str, clusterNames: dict | None = None) -> None:
        df = self.df[self.clusterCols]
        scaler = StandardScaler()
        Xscaled = scaler.fit_transform(df.to_numpy())
        self.__compareK(Xscaled=Xscaled, fileName="appendix_compare_K_{}.jpg".format(saveName))
        _, df = self.__kmeans(k, Xscaled, self.clusterCols, saveName, clusterNames=clusterNames)
        self.df["flooding"] = df["cluster"]
        
        self.correlation()

        return
    
    def classAcc(self, cols: list[str], content: str, k: int) -> pd.Series | pd.DataFrame:
        df = self.df[cols]
        scaler = StandardScaler()
        Xscaled = scaler.fit_transform(df.to_numpy())
        self.__compareK(Xscaled=Xscaled, fileName="appendix_compare_K_{}.jpg".format(content))
        _, df = self.__kmeans(k, Xscaled, cols, "clustering_{}".format(content))

        # Find the interval
        df.sort_values(cols, ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
        for i in range(1, len(df)):
            if df.loc[i, "cluster"] != df.loc[i-1, "cluster"]:
                return df.loc[i][cols]

        return pd.DataFrame()
    
    def __getChangeThres(self, col: str, changeThres: str) -> float:
        if changeThres == "cluster":
            return self.classAcc([col], col, 2)[[col]].values[0]
        elif changeThres == "mean":
            return self.df[self.df[col] != 0][col].mean()
        else:
            return self.df[self.df[col] != 0][col].median()

    def groupAnalysis(self, changThres: str) -> None:
        savePath = os.path.join(self.savePath, changThres)
        mkdir(savePath)

        def generateDf(df: pd.DataFrame, x1: float, x2: float, cluster: str, savePath: str) -> None:
            accResults = ["Pop Good", "Pop Bad", "Facility Good", "Facility Bad"]
            # Count profermance
            df["Pop Good"] = df["A_All_change_all"] > x1
            df["Pop Bad"] = df["A_All_change_all"] <= x1
            df["Facility Good"] = df["A_POIAll_change_all"] > x2
            df["Facility Bad"] = df["A_POIAll_change_all"] <= x2

            grouped = df[
                [cluster] + self.clusterCols + accResults
            ].groupby(cluster)
            result = pd.concat(
                [
                    grouped[self.clusterCols].agg(["mean", "median"]),
                    grouped.size().rename("count"),
                    grouped[accResults].sum(),
                    grouped[accResults].mean().mul(100).add_suffix("_prop")
                ],
                axis=1
            )
            result["pop-thres"] = x1
            result["facility-thres"] = x2
            result.to_csv(os.path.join(savePath, "flooding.csv"), float_format="%.4f")

            return

        # Count profermance
        x1 = self.__getChangeThres("A_All_change_all", changThres)
        x2 = self.__getChangeThres("A_POIAll_change_all", changThres)
        generateDf(self.df, x1, x2, "flooding", savePath)

        grouped = self.df.groupby("flooding")
        total = grouped.size()

        # Results
        for x in ["Pop", "Facility"]:
            goodCol = "{} Good".format(x)
            badCol = "{} Bad".format(x)

            # Radae
            subplot = plt.subplot("WN32", 1, 3, legend=False, projection="polar")
            axs = subplot.axs
            for group, df in grouped:
                good = df[df[goodCol]][self.__INDICATOR_COLS].mean().to_numpy()
                bad = df[df[badCol]][self.__INDICATOR_COLS].mean().to_numpy()
                ax = axs[int(group)] # type: ignore
                self.__plotRadar(
                    ax,
                    good / bad - 1,
                    self.__INDICATOR_COLS,
                    BAR_COLORS[0][0],
                    str(group)
                )
                ax.set_xlabel("Cluster {}".format(group))
        
            plt.plot(savePath, "indicator_{}".format(x), fig=subplot.fig)

            # Stacked
            fig, ax = plt.figure("W31")
            ratioGood = (grouped[goodCol].sum() / total * 100).fillna(0)
            ratioBad  = (grouped[badCol].sum()  / total * 100).fillna(0)
            groupLabels = ratioGood.index.astype(str)
            ax.bar(groupLabels, ratioGood, label=f"{x} Good", color=BAR_COLORS[1][0])
            ax.bar(groupLabels, ratioBad, bottom=ratioGood, label=f"{x} Bad", color=BAR_COLORS[1][1])
            ax.set_xlabel("Flooding group")
            ax.set_ylabel("Proportion of cities (%)")
            ## Label
            for i, (_, t) in enumerate(zip(groupLabels, total)):
                goodVal = ratioGood[i]
                badVal = ratioBad[i]
                # Percentage label
                if goodVal > 0:
                    ax.text(i, goodVal / 2, f"{goodVal:.2f}%", ha="center", va="center", fontsize=NOTE_SIZE, color="black")
                if badVal > 0:
                    ax.text(i, goodVal + badVal / 2, f"{badVal:.2f}%", ha="center", va="center", fontsize=NOTE_SIZE, color="black")
                # Total count label
                ax.text(i, 102, f"n={t}", ha="center", va="bottom", fontsize=NOTE_SIZE)

            plt.plot(savePath, "stakeed_{}".format(x), fig=fig)
            
        return
    
    def __plotRadar(self, ax: plt.Axes, data: np.ndarray, categories: list, color: str, label: str) -> None:
        N = len(categories)
        angles = np.linspace(0, 2 * np.pi, N, endpoint=False).tolist()
        data = np.concatenate((data, [data[0]]))  # Close data for radar
        angles += angles[:1]
        ax.plot(angles, data, 'o-', linewidth=2, color=color, label=label)
        ax.fill(angles, data, alpha=0.25, color=color)
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels([textwrap.fill(self.__STAND_NAME[x], width=10, break_long_words=False) for x in categories])
        ax.axhline(y=0, color="red")
        ax.set_ylim(-1, 1)
        ax.set_yticks([-1, -0.5, 0, 0.5, 1])
        ax.yaxis.set_major_formatter(PercentFormatter(1.0, symbol=None))  # Change y axis to percentage

        return
    
    # Correlation
    def correlation(self) -> None:
        plots = plt.subplot("W", 1, 2, legend=True, sharey=True)
        axs = plots.axs
        for i, _ in enumerate(("Population-based", "Facility-based")):
            colAll = self.COL.all[i]
            df = self.df[["flooding", colAll] + self.__INDICATOR_COLS].copy()

            self.__correlation(df, colAll, "flooding", axs[i])
        
        plots.legend(ncol=len(self.__INDICATOR_COLS)//2)
        plt.plot(self.savePath, "correlation.jpg")

        return

    def __correlation(self, df: pd.DataFrame, yCol: str, cluster: str, ax: plt.Axes) -> None:
        results = {}
        pValues = {}
        indicators = self.__INDICATOR_COLS[:]

        scaler = StandardScaler()
        df = df.copy()
        df.loc[:, indicators] = scaler.fit_transform(df[indicators])
        
        for clusterID, subset in df.groupby(cluster):
            # Not enough sample
            if len(subset) < len(indicators) + 5:
                print(f"Waring：Cluster {clusterID} do not have enough sample ({len(subset)}).")
            
            X = subset[indicators]
            y = subset[yCol]
            X = sm.add_constant(X)
            model = sm.OLS(y, X).fit()
            results[clusterID] = model
            pValues[clusterID] = model.pvalues[indicators]  # 只保留可调整指标的p值
            
            print(f" --- Cluster {clusterID} (n={len(subset)}) ---")
            print(model.summary())

        # Save and plot
        coef = pd.DataFrame({cid: model.params[indicators] for cid, model in results.items()}).T
        blank = 0.01 * (max(coef.max()) - min(coef.min()))
        pValue = pd.DataFrame(pValues).T
        
        # Plot
        x = np.arange(len(coef.index))
        width = 0.15
        
        for i, var in enumerate(indicators):
            coeffs = np.array(coef[var].values)
            pvals = pValue[var].values
            # Drwa significant
            sigStars = [significanceStars.sign(p) for p in pvals]
            
            bars = ax.bar(x + i*width, coeffs, width, label=self.__STAND_NAME[var])
            
            for bar, star in zip(bars, sigStars):
                if star:
                    height = bar.get_height()
                    ax.text(
                        bar.get_x() + bar.get_width()/2, height + blank if height >= 0 else height - 5*blank,
                        star,
                        ha="center", va="bottom",
                        fontsize=NOTE_SIZE
                    )
        
        ax.axhline(0, color="black", linewidth=0.8, linestyle="--")
        ax.set_xlabel("Cluster")
        ax.set_ylabel("Coefficient")
        ax.set_xticks(x + width * (len(indicators)-1)/2, labels=coef.index)
        
        return

if __name__ == "__main__":
    import os
    ANALY_RESULT = r"C:\0_PolyU\test\3km"
    INDICATOR = r"C:\\0_PolyU\\test\\indicator.gpkg"

    cluster = clustering(os.path.join(ANALY_RESULT, "changeRatio_result.csv"), INDICATOR, clusterCols=["floodingCoverage", "floodingDisparity"], savePath=ANALY_RESULT)
    cluster.run(
        k=3, saveName="clustering_flooding",
        clusterNames={
            0: "Concentrated in population, Low flood coverage",
            1: "Not concentrated in population, Low Flood Coverage",
            2: "Not concentrated in population, High Flood Coverage"
        }
    ) # Cluster flooding
    cluster.groupAnalysis(changThres="cluster")
    cluster.groupAnalysis(changThres="mean")
    cluster.groupAnalysis(changThres="median")