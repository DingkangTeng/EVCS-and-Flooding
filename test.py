# test
from nodeCalculate.FCA.M2SFCA import gridBasedM2SFCA
df = gridBasedM2SFCA((r"C:\0_PolyU\roadsGraph_BeijinInner\global_EVCS.gpkg", "evcs"), r"C:\0_PolyU\roadsGraph_BeijinInner\BJ_Pop.tif").cal(1000, withDemandValue=True)
# df.to_file(r"C:\0_PolyU\roadsGraph_BeijinInner\test.gpkg", layer="test")
df.to_csv(r"C:\0_PolyU\roadsGraph_BeijinInner\test_grid_based.csv")