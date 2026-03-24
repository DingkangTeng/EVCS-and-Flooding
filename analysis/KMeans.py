import sys
import pandas as pd
import geopandas as gpd
import numpy as np
import seaborn as sns
import statsmodels.api as sm
from matplotlib.ticker import PercentFormatter
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler, StandardScaler, RobustScaler
from sklearn.metrics import silhouette_score

sys.path.append(".") # Set path to the roots

from _function.readChangeRatio import readChangeRatio
from _function.readFiles import mkdir
from _plot import plt, BAR_COLORS

class clustering:
    __slots__ = [
        "df", "COL", "cities",
        "floodDf",
        "Xscaled", "savePath"
    ]

    __CLUST_INDICATOR_COLS = [
        "EVCSPop", "EVCSConnectivity"
    ]

    # __CLUST_INDICATOR_COLS = [
    #     "EVCSAggregation", "EVCSPopCover",
    #     "roadDensity",
    #     "floodingCoverage", "floodingDisparity"
    # ]
    
    __INDICATOR_COLS = [
        "EVCSDensity", "EVCSAggregation",
        "EVCSConnectivity", "EVCSPopCover", "EVCSPop"
    ]

    __STAND_NAME = {
        "A_All_change_all": "Pop.-based accessibility change ratio (%)",
        "A_POIAll_change_all": "Facility-based accessibility change ratio (%)",
        "EVCSDensity": "EVCS Density",
        "EVCSAggregation": "EVCS aggregation (NNI)",
        "EVCSConnectivity": "Average road counts per EVCS",
        "EVCSPopCover": "EVCS-Pop. coverage",
        "EVCSPop": "EVCS-Pop. disparity",
        "roadDensity": "Road density (km/km²)",
        "floodingCoverage": "Flooding coverage (%)",
        "floodingDisparity": "Flooding-Pop. disparity"
    }

    def __init__(self, path: str, indicator: str, savePath: str) -> None:
        self.df, self.COL = readChangeRatio(path, indexCity=True, initial=True)
        indicatorDf = gpd.read_file(indicator, layer="city", encoding="utf-8").set_index("city")[
            list(self.__STAND_NAME.keys())[2:]
        ].dropna()
        self.df = self.df.join(indicatorDf).dropna()
        
        indicatorDf = indicatorDf[self.__CLUST_INDICATOR_COLS]
        scaler = StandardScaler()
        self.Xscaled = scaler.fit_transform(indicatorDf.to_numpy())
        self.cities = indicatorDf.index
        self.savePath = os.path.join(savePath, "clustering")

        # Cluster flooding
        floodingCols = ["floodingCoverage", "floodingDisparity"]
        floodingDf = self.df[floodingCols]
        scaler = StandardScaler()
        Xscaled = scaler.fit_transform(floodingDf.to_numpy())
        self.compareK(Xscaled=Xscaled, fileName="appendix_compare_K_flooding.jpg")
        _, self.floodDf = self.__kmeans(3, Xscaled, floodingCols, "clustering_flooding")
        self.df["flooding"] = self.floodDf["cluster"]

        mkdir(self.savePath)

        # # Correlation
        # from analysis.regression import autoCorrelation
        # autoCorrelation(self.df, self.__INDICATOR_COLS, [], True)

        del indicatorDf

        return

    def compareK(
        self,
        maxTry: int = 10, Xscaled: np.ndarray | None = None, fileName: str = "appendix_compare_K.jpg"
    ) -> None:
        plots = plt.subplot("W", 1, 2, legend=False)
        axs = plots.axs
        Xscaled = self.Xscaled if Xscaled is None else Xscaled

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
        saveName: str, index: pd.Index | None = None
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
        plots = plt.subplot("W" if col > 1 else "D", row, col)
        for i in range(0, row*col):
            ax = plots.axs[i]
            x = cols[2*i if total%2==0 else i]
            y = cols[2*i+1 if total%2==0 else i+1] if total > 1 else "A_All"
            sns.scatterplot(
                data=df,
                x=x, y=y,
                ax=ax,
                hue="cluster", palette="Set1"
            )
            ax.set_xlabel(self.__STAND_NAME[x])
            ax.set_ylabel(self.__STAND_NAME[y])
        plots.legend(title="Clustering number", ncol=k)
        plt.plot(self.savePath, "{}.jpg".format(saveName))

        return clusters, df
    
    def classAcc(self, cols: list[str], content: str, k: int) -> pd.Series | pd.DataFrame:
        df = self.df[cols]
        scaler = StandardScaler()
        Xscaled = scaler.fit_transform(df.to_numpy())
        self.compareK(Xscaled=Xscaled, fileName="appendix_compare_K_{}.jpg".format(content))
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
            return self.classAcc([col], "pop", 2)[[col]].values[0]
        elif changeThres == "mean":
            return self.df[self.df[col] != 0][col].mean()
        else:
            return self.df[self.df[col] != 0][col].median()

    def run(self, k: int, changThres: str) -> None:
        def generateDf(df: pd.DataFrame, x1: float, x2: float, savePath: str) -> None:
            accResults = ["Pop Good", "Pop Bad", "Facility Good", "Facility Bad"]
            # Count profermance
            df["Pop Good"] = df["A_All_change_all"] > x1
            df["Pop Bad"] = df["A_All_change_all"] <= x1
            df["Facility Good"] = df["A_POIAll_change_all"] > x2
            df["Facility Bad"] = df["A_POIAll_change_all"] <= x2

            grouped = df[
                ["cluster"] + self.__CLUST_INDICATOR_COLS + accResults
            ].groupby("cluster")
            result = pd.concat(
                [
                    grouped[self.__CLUST_INDICATOR_COLS].agg(["mean", "median"]),
                    grouped.size().rename("count"),
                    grouped[accResults].sum(),
                    grouped[accResults].mean().mul(100).add_suffix("_prop")
                ],
                axis=1
            )
            result["pop-thres"] = x1
            result["facility-thres"] = x2
            result.to_csv(savePath, float_format="%.4f")

            return

        # Count profermance
        x1 = self.__getChangeThres("A_All_change_all", changThres)
        x2 = self.__getChangeThres("A_POIAll_change_all", changThres)
        generateDf(self.floodDf, x1, x2, os.path.join(self.savePath, "flooding_cluster_{}.csv".format(changThres)))

        for name, df in self.df.groupby("flooding"):
            scaler = StandardScaler()
            Xscaled = scaler.fit_transform(df[self.__CLUST_INDICATOR_COLS].to_numpy())
            self.compareK(Xscaled=Xscaled, fileName="appendix_compare_K_{}".format(name))
            clusters, df = self.__kmeans(
                k, Xscaled, self.__CLUST_INDICATOR_COLS, "flooding_cluster_{}".format(name), df.index
            )

            newSave = os.path.join(self.savePath, "flooding_cluster_{}".format(name), changThres)
            
            mkdir(newSave)

            # Count profermance
            generateDf(df, x1, x2, os.path.join(newSave, "clustering.csv"))
            
            # Different influence in different group
            portionPlots = plt.subplot("W", 1, 2, sharey=True, sharex=True)
            portionAxs = portionPlots.axs

            for i, name in enumerate(("Population-based", "Facility-based")):
                colEVCS = self.COL.EVCS[i]
                colRoad = self.COL.road[i]
                colAll = self.COL.all[i]

                df["diff"] = df[colEVCS].abs() - df[colRoad].abs()
                df["winner"] = "road"
                df.loc[df["diff"] > 0, "winner"] = "EVCS"
                df.loc[df["diff"] == 0, "winner"] = "tie"

                self.correlation(df.copy(), colAll)

                # Plot
                ax = portionAxs[i]
                ## Cross table
                ct = pd.crosstab(df["cluster"], df["winner"])
                ct = ct.div(ct.sum(axis=1), axis=0)  # Normalize by row
                ct.plot(kind="bar", stacked=True, ax=ax, color=BAR_COLORS[0])
                ax.set_xlabel("Cluster")
                ax.set_ylabel("Proportion (%)")
                ax.yaxis.set_major_formatter(PercentFormatter(1.0, symbol=None))  # Change y axis to percentage

            portionPlots.legend(ncol=k)
            plt.plot(newSave, "clusting_portion", fig=portionPlots.fig)
            
        return
    
    @staticmethod
    def __plotRadar(ax: plt.Axes, data: np.ndarray, categories: list, color: str, label: str) -> None:
        N = len(categories)
        angles = np.linspace(0, 2 * np.pi, N, endpoint=False).tolist()
        data = np.concatenate((data, [data[0]]))  # Close data for radar
        angles += angles[:1]
        ax.plot(angles, data, 'o-', linewidth=2, color=color, label=label)
        ax.fill(angles, data, alpha=0.25, color=color)
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(categories)
        ax.axhline(y=0, color="red")
        ax.set_ylim(-1, 1)

        return

    def correlation(self, df: pd.DataFrame, yCol: str) -> None:
        results = {}
        pvalues_dict = {}  # 存储每个簇的p值
        indicators = self.__INDICATOR_COLS[1:]

        scaler = StandardScaler()
        df = df.copy()
        df.loc[:, indicators] = scaler.fit_transform(df[indicators])
        
        for cluster_id in sorted(df['cluster'].unique()):
            subset = df[df['cluster'] == cluster_id]
            if len(subset) < len(indicators) + 5:  # 样本量过少时跳过或警告
                print(f"警告：簇 {cluster_id} 样本量 {len(subset)} 偏少，结果可能不可靠")
            
            X = subset[indicators]
            y = subset[yCol]
            X = sm.add_constant(X)
            model = sm.OLS(y, X).fit()
            results[cluster_id] = model
            pvalues_dict[cluster_id] = model.pvalues[indicators]  # 只保留可调整指标的p值
            
            print(f" --- Cluster {cluster_id} (n={len(subset)}) ---")
            # 打印系数表（包含p值）
            print(model.summary().tables[1])

        # 提取系数和p值，构建DataFrame
        coef_df = pd.DataFrame({cid: model.params[indicators] for cid, model in results.items()}).T
        pval_df = pd.DataFrame(pvalues_dict).T
        
        # 绘图
        fig, ax = plt.figure("D")
        x = np.arange(len(coef_df.index))  # 簇的序号位置
        width = 0.15  # 柱宽
        
        for i, var in enumerate(indicators):
            coeffs = coef_df[var].values
            pvals = pval_df[var].values
            # 判断显著性
            sig_stars = []
            for p in pvals:
                if p < 0.001:
                    sig_stars.append('***')
                elif p < 0.01:
                    sig_stars.append('**')
                elif p < 0.05:
                    sig_stars.append('*')
                else:
                    sig_stars.append('')
            
            # 绘制柱子
            bars = ax.bar(x + i*width, coeffs, width, label=var)
            
            # 在柱子上方添加星号
            for j, (bar, star) in enumerate(zip(bars, sig_stars)):
                if star:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., 
                            height + 0.01 * (max(coef_df.max()) - min(coef_df.min())), 
                            star, ha='center', va='bottom', fontsize=12, fontweight='bold')
        
        ax.axhline(0, color='black', linewidth=0.8, linestyle='--')
        ax.set_xlabel('Cluster')
        ax.set_ylabel('Coefficient')
        ax.set_xticks(x + width * (len(indicators)-1)/2)
        ax.set_xticklabels(coef_df.index)
        ax.legend(loc='best')
        
        plt.plot(self.savePath, "correlation_{}.jpg".format(yCol), fig)
        
        return

if __name__ == "__main__":
    import os
    ANALY_RESULT = r"C:\0_PolyU\test\3km"
    INDICATOR = r"C:\\0_PolyU\\test\\indicator.gpkg"

    a = clustering(os.path.join(ANALY_RESULT, "changeRatio_result.csv"), INDICATOR, savePath=ANALY_RESULT)
    # a.classAcc(["A_All_change_all", "A_POIAll_change_all"], "pop", 2) #"A_All"
    a.run(k=3, changThres="cluster")
    a.run(k=3, changThres="mean")
    a.run(k=3, changThres="median")