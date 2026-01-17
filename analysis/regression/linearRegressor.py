import sys, os
import geopandas as gpd
import numpy as np
import statsmodels.api as sm
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression

sys.path.append(".") # Set path to the roots

from analysis.regression import autoCorrelation
from analysis.__statisticalDiff import significanceStars
from _plot import plt, BAR_COLORS, NOTE_SIZE

# class linearRegressor(autoCorrelation):
    
#     def linearRegression(self) -> None:
#         subplot = plt.subplot("W", len(self.Y_COL), len(self.X_COL), legend=True)

#         for i, ycol in enumerate(self.Y_COL):
#             Y = self.df[ycol].to_numpy()
            
#             for j, xcol in enumerate(self.X_COL):
#                 X = self.df[xcol].to_numpy().reshape(-1, 1)
#                 model = LinearRegression()
#                 model.fit(X, Y)
#                 yPred = model.predict(X)
                
#                 # 计算指标
#                 n = len(Y)
#                 r2 = r2_score(Y, yPred)
#                 rmse = np.sqrt(mean_squared_error(Y, yPred))
#                 correlation = np.corrcoef(X.flatten(), Y)[0, 1]
                
#                 # 计算p值
#                 if n > 2:
#                     residuals = Y - yPred
#                     sse = np.sum(residuals ** 2)
#                     sst = np.sum((Y - np.mean(Y)) ** 2)
#                     msr = sst - sse
#                     mse_res = sse / (n - 2)
#                     f_stat = msr / mse_res
#                     p = float(1 - stats.f.cdf(f_stat, 1, n - 2))
#                 else:
#                     p = 1.0
                
#                 # 创建图形
#                 ax = subplot.axs[i * len(self.X_COL) + j]
                
#                 # 绘制散点图和回归线
#                 ax.scatter(X, Y, alpha=0.7, s=80, label="Data Points", color=BAR_COLORS[0][i+j])
                
#                 # 回归线
#                 x_plot = np.linspace(X.min(), X.max(), 100).reshape(-1, 1)
#                 y_plot = model.predict(x_plot)
#                 ax.plot(x_plot, y_plot, "r-", linewidth=3, label="Regression Line", color=BAR_COLORS[1][i+j])
                
#                 # 设置图形属性
#                 ax.set_xlabel(xcol)
#                 ax.set_ylabel(ycol)
#                 ax.set_title(f"{ycol} vs {xcol}")
#                 ax.grid(True, alpha=0.3)
                
#                 # 在图中添加统计信息
#                 statsText = f"y = {model.intercept_:.3f} + {model.coef_[0]:.3f}x\n"
#                 statsText += f"R² = {r2:.4f}\n"
#                 statsText += f"r = {correlation:.4f}\n"
#                 statsText += f"RMSE = {rmse:.4f}\n"
#                 statsText += f"p = {p:.4f} ({significanceStars.sign(float(p))})"
                
#                 # 添加文本框
#                 ax.text(
#                     0.55, 0.25, statsText, transform=ax.transAxes,
#                     verticalalignment="top",
#                     fontsize=NOTE_SIZE,
#                     bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.8))
                
#                 # 控制台输出
#                 print(f"\n{ycol} ~ {xcol}")
#                 print(f"  方程: {ycol} = {model.intercept_:.3f} + {model.coef_[0]:.3f} × {xcol}")
#                 print(f"  R² = {r2:.3f}, r = {correlation:.3f}, RMSE = {rmse:.3f}")
#                 print(f"  n = {n}, p = {p:.4f}")
        
#         subplot.fig.legend(
#             handles=subplot.axs[0].get_legend_handles_labels()[0],
#             loc="lower center",
#             ncol=3,
#             frameon=True,
#             fancybox=True,
#             borderpad=1,
#             labelspacing=0.5,
#             handletextpad=1,
#             columnspacing=1.5
#         )
#         plt.plot()

#         return

class linearRegressor(autoCorrelation):
    
    def linearRegression(self, figsize: str = "WN31", savePath: str = "") -> None:
        X = self.df[self.X_COL]
        y = self.df[self.Y_COL].to_numpy().flatten()

        # Add constant term for intercept
        Xsm = sm.add_constant(X)
        model = sm.OLS(y, Xsm)
        results = model.fit()
        yPred = results.predict(Xsm)

        # Calculate standardized coefficients for variable importance
        scaler = StandardScaler()
        XScaled = scaler.fit_transform(X)
        yScaled = (y - y.mean()) / y.std()
        model_scaled = LinearRegression()
        model_scaled.fit(XScaled, yScaled)
        
        multiplot = plt.subplot(figsize, 1, 2, widthRatios=[1, 1], legend=False)
        axs = multiplot.axs

        # Regression equation and statistics summary
        ## Equation
        equationText = f"Regression Equation:\n"
        equationText += f"Y = {results.params["const"]:.4f} "
        for var in self.X_COL:
            equationText += f"+ {results.params[var]:.4f} × {var} "

        ## Model evaluation text
        statsText = f"Model Evaluation:\n"
        statsText += f"R² = {results.rsquared:.4f}\n"
        statsText += f"Adjusted R² = {results.rsquared_adj:.4f}\n"
        statsText += f"F-statistic = {results.fvalue:.2f}\n"
        statsText += f"F-test p-value = {results.f_pvalue:.4f}\n"
        statsText += f"Sample Size = {int(results.nobs)}"

        ## Variable significance text
        coefText = "\nVariable Significance:"
        for var in self.X_COL:
            p = results.pvalues[var]
            coefText += f"\n{var}: β={results.params[var]:.4f}, p={p:.4f} ({significanceStars.sign(p)})"

        ## Add text
        print(equationText + "\n" + coefText + "\n" + statsText)

        # variable importance bar chart
        ax3 = axs[0]
        coefValues = model_scaled.coef_
        if coefValues.ndim > 1:
            coefValues = coefValues.flatten()
        coefAbs = np.abs(coefValues)

        ## Bars
        colors = BAR_COLORS[0][:len(self.X_COL)]
        bars = ax3.bar(self.X_COL, coefAbs, color=colors, alpha=0.8, edgecolor="black", linewidth=1.2)

        ## Add value labels on bars
        for i, (bar, coef) in enumerate(zip(bars, coefValues)):
            height = bar.get_height()
            ax3.text(
                bar.get_x() + bar.get_width()/2., height + 0.01,
                f"{coef:.4f} ({significanceStars.sign(results.pvalues[self.X_COL[i]])})",
                ha="center", va="bottom", fontsize=NOTE_SIZE, fontweight="bold"
            )

        ax3.set_ylabel("Absolute Standardized Coefficient")

        # Residual analysis scatter plot
        ax4 = axs[1]
        residuals = y - yPred

        ## Scatter plot with residuals as color
        scatter = ax4.scatter(
            yPred, y, c=residuals,
            cmap="coolwarm_r", alpha=0.7, edgecolors='k', linewidth=0.5, s=60
        )

        # Add y=x reference line
        minVal = min(y.min(), yPred.min())
        maxVal = max(y.max(), yPred.max())
        ax4.plot([minVal, maxVal], [minVal, maxVal], 'k--', linewidth=1.5, label="y=x Reference")

        # Add colorbar and labels and r2
        cbar = plt.plt.colorbar(scatter, ax=ax4)
        cbar.set_label("Residuals")
        ax4.set_xlabel("Predicted EVCS Change")
        ax4.set_ylabel("Actual EVCS Change")
        ax4.text(
            0.05, 0.95,
            f'R² = {results.rsquared:.4f}',
            transform=ax4.transAxes,
            fontsize=NOTE_SIZE, verticalalignment="top",
            bbox=dict(boxstyle="round", facecolor="white", alpha=0.8)
        )

        plt.plot(savePath, "LinearRegression_Summary.jpg")

        # 3. 打印详细的回归结果
        print("=" * 70)
        print("多元线性回归详细结果")
        print("=" * 70)
        print(results.summary())

        # 4. 打印简单解释
        print("\n" + "=" * 70)
        print("结果解释:")
        print("=" * 70)
        for var in self.X_COL:
            print(f"1. {var}每增加1%，EVCS变化率平均增加 {results.params[var]:.3f}%")
        print(f"3. 模型解释了EVCS变化率变异的 {results.rsquared*100:.1f}%")
        print(f"4. 标准化系数显示{'EVCS覆盖率' if abs(model_scaled.coef_[0]) > abs(model_scaled.coef_[1]) else '洪水覆盖率'}对变化率影响更大")

        return

# Debug
if __name__ == "__main__":
    ANALY_RESULT_ROOT = r"C:\\0_PolyU\\test"
    INDICATOR = os.path.join(ANALY_RESULT_ROOT, "indicator.gpkg")

    EVCSRegCol = [
        "EVCScoverage", "folldingCoverage"
    ]
    df = gpd.read_file(INDICATOR, layer="city")[EVCSRegCol + ["city", "EVCSChange", "geometry"]]
    a = linearRegressor(df, EVCSRegCol, ["EVCSChange"], checkCorelation=False)
    a.linearRegression(savePath=ANALY_RESULT_ROOT)
    # a.spatialAuto()