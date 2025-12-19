import sys, os
import geopandas as gpd
import shap
from sklearn.ensemble import RandomForestRegressor

sys.path.append(".") # Set path to the roots

from analysis.__readNode import readNode
from analysis.__setting import STAND_NAME, AColumns
from analysis.__calRatio import calRatio

X_COL = ["EVCScoverage", "EVCSDensity", "roadDensity", "roadCoverage", "populationDensity", "populationCoverage", "folldingCoverage"]

def rfRegressor(result: str, indicator: str) -> None:
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

    gdf = gpd.read_file(indicator)[X_COL + ["city"]].set_index("city")
    df = df.join(gdf).dropna()

    X = df[X_COL]
    for ycol in ratio:
        Y = df[ycol]
        model = RandomForestRegressor()
        model.fit(X, Y)

        # SHAP
        explainer = shap.TreeExplainer(model)
        shapValue = explainer.shap_values(X)
        print(ycol)
        shap.summary_plot(shapValue, X, title=ycol)

    return

# Debug
if __name__ == "__main__":
    root = r"C:\\0_PolyU\\test"
    city = os.path.join(root, "1km", "city.csv")
    indicator = os.path.join(root, "indicator.gpkg")
    rfRegressor(city, indicator)