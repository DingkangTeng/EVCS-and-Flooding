import sys, os, shap
import geopandas as gpd
from sklearn.ensemble import RandomForestRegressor

sys.path.append(".") # Set path to the roots

from analysis.__readNode import readNode
from analysis.__setting import AColumns
from analysis.__calRatio import calRatio
from analysis.regression import autoCorrelation

class rfRegressor(autoCorrelation):
    
    def __init__(self, result: str, indicator: str, xcol: list[str], checkCorelation: bool = True) -> None:
        A_BEFORE = []
        A_AFTER = []
        for analysisType in ["popStatic", "popDynamic", "POI"]:
            A_ACC_BEFORE, A_ACC_AFTER = AColumns(analysisType, "accessibility")
            A_EQU_BEFORE, A_EQU_AFTER = AColumns(analysisType, "equity")
            # Only consider the overall demograpic/POI
            A_BEFORE += [A_ACC_BEFORE[-1], A_EQU_BEFORE[-1]]
            A_AFTER += [A_ACC_AFTER[-1], A_EQU_AFTER[-1]]
        addCol = ["iso3", "EVCSNum"]
        
        df, _, _ = readNode(result)
        df = df[A_BEFORE + A_AFTER + addCol + ["city"]].set_index("city")

        df, ratio = calRatio(df, A_BEFORE, A_AFTER)
        df: gpd.pd.DataFrame = df[addCol + ratio.tolist()]

        gdf = gpd.read_file(indicator, layer="city")[xcol + ["city", "geometry"]].set_index("city")
        df = gpd.GeoDataFrame(df.join(gdf).dropna(), crs=gdf.crs)
        del gdf

        super().__init__(df, xcol, ratio, checkCorelation)

        return

    def rfRegression(self) -> None:
        X = self.df[self.X_COL]
        for ycol in self.Y_COL:
            Y = self.df[ycol]
            model = RandomForestRegressor()
            model.fit(X, Y)

            # SHAP
            explainer = shap.TreeExplainer(model)
            shapValue = explainer.shap_values(X)
            print(ycol)
            shap.summary_plot(shapValue, X, title=ycol)
            shap.dependence_plot("EVCSDensity", shapValue, X, interaction_index=None)

        return

# Debug
if __name__ == "__main__":
    root = r"C:\\0_PolyU\\test"
    CITY_RESULT = os.path.join(root, "3km", "city.csv")
    INDICATOR = os.path.join(root, "indicator.gpkg")

    FloodingRegCol = [
        "EVCScoverage", "EVCSDensity",
        "roadDensity", "roadCoverage",
        "populationDensity", "populationCV",
        "folldingCoverage"
    ]
    
    a = rfRegressor(CITY_RESULT, INDICATOR, FloodingRegCol, checkCorelation=False)
    a.dropCorelation({"roadDensity", "EVCScoverage"}, checkCorelation=False)
    # a.spatialAuto()
    a.rfRegression()