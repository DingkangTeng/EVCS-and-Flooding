import sys, os
import geopandas as gpd
import numpy as np
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_squared_error

sys.path.append(".") # Set path to the roots

from analysis.regression import autoCorrelation
from analysis.__statisticalDiff import significanceStars
from _plot import plt, BAR_COLORS, NOTE_SIZE

class linearRegressor(autoCorrelation):
    
    def linearRegression(self) -> None:
        subplot = plt.subplot("W", len(self.Y_COL), len(self.X_COL), legend=True)

        for i, ycol in enumerate(self.Y_COL):
            Y = self.df[ycol].to_numpy()
            
            for j, xcol in enumerate(self.X_COL):
                X = self.df[xcol].to_numpy().reshape(-1, 1)
                model = LinearRegression()
                model.fit(X, Y)
                Y_pred = model.predict(X)
                
                # 计算指标
                n = len(Y)
                r2 = r2_score(Y, Y_pred)
                rmse = np.sqrt(mean_squared_error(Y, Y_pred))
                correlation = np.corrcoef(X.flatten(), Y)[0, 1]
                
                # 计算p值
                if n > 2:
                    residuals = Y - Y_pred
                    sse = np.sum(residuals ** 2)
                    sst = np.sum((Y - np.mean(Y)) ** 2)
                    msr = sst - sse
                    mse_res = sse / (n - 2)
                    f_stat = msr / mse_res
                    p = float(1 - stats.f.cdf(f_stat, 1, n - 2))
                else:
                    p = 1.0
                
                # 创建图形
                ax = subplot.axs[i * len(self.X_COL) + j]
                
                # 绘制散点图和回归线
                ax.scatter(X, Y, alpha=0.7, s=80, label="Data Points", color=BAR_COLORS[0][i+j])
                
                # 回归线
                x_plot = np.linspace(X.min(), X.max(), 100).reshape(-1, 1)
                y_plot = model.predict(x_plot)
                ax.plot(x_plot, y_plot, "r-", linewidth=3, label="Regression Line", color=BAR_COLORS[1][i+j])
                
                # 设置图形属性
                ax.set_xlabel(xcol)
                ax.set_ylabel(ycol)
                ax.set_title(f"{ycol} vs {xcol}")
                ax.grid(True, alpha=0.3)
                
                # 在图中添加统计信息
                stats_text = f"y = {model.intercept_:.3f} + {model.coef_[0]:.3f}x\n"
                stats_text += f"R² = {r2:.4f}\n"
                stats_text += f"r = {correlation:.4f}\n"
                stats_text += f"RMSE = {rmse:.4f}\n"
                stats_text += f"p = {p:.4f} ({significanceStars.sign(float(p))})"
                
                # 添加文本框
                ax.text(
                    0.55, 0.25, stats_text, transform=ax.transAxes,
                    verticalalignment="top",
                    fontsize=NOTE_SIZE,
                    bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.8))
                
                # 控制台输出
                print(f"\n{ycol} ~ {xcol}")
                print(f"  方程: {ycol} = {model.intercept_:.3f} + {model.coef_[0]:.3f} × {xcol}")
                print(f"  R² = {r2:.3f}, r = {correlation:.3f}, RMSE = {rmse:.3f}")
                print(f"  n = {n}, p = {p:.4f}")
        
        subplot.fig.legend(
            handles=subplot.axs[0].get_legend_handles_labels()[0],
            loc="lower center",
            ncol=3,
            frameon=True,
            fancybox=True,
            borderpad=1,
            labelspacing=0.5,
            handletextpad=1,
            columnspacing=1.5
        )
        plt.plot()

        return

# Debug
if __name__ == "__main__":
    root = r"C:\\0_PolyU\\test"
    INDICATOR = os.path.join(root, "indicator.gpkg")

    EVCSRegCol = [
        "EVCScoverage", "folldingCoverage"
    ]
    df = gpd.read_file(INDICATOR, layer="city")[EVCSRegCol + ["city", "EVCSChange", "geometry"]]
    a = linearRegressor(df, EVCSRegCol, ["EVCSChange"], checkCorelation=False)
    a.linearRegression()
    a.spatialAuto()

    # GEO_DB = r"_GISAnalysis\Dissertation.gdb"
    # # col = ["allPop_accChange", "allPOI_accChange"]
    # col = ["allPop_equChange", "allPOI_equChange"]
    # df = gpd.read_file(GEO_DB, layer="globalCity")[col + ["EVCS_Change", "geometry"]]
    # a = linearRegressor(df, col, ["EVCS_Change"], checkCorelation=False)
    # a.linearRegression()