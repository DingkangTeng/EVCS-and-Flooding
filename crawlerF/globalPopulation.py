# It seems that popWorld do not support mutlti-thread downloading?

import sys, os
import pandas as pd
from bs4 import BeautifulSoup as bs

sys.path.append(".") # Set path to the roots

from crawlerF.crawler import crawler
from function.otherFunction import mkdir

class globalPopulation(crawler):
    __url = "https://hub.worldpop.org/ajax/geolisting/category?id=88"
    __countries = []
    __indexC = []

    def __init__(self):
        self.getAllCountries()
    
    def getAllCountries(self) -> None:
        super().__init__(self.__url)
        r = self.rget()
        self.__countries = r.json()
        for x in self.__countries:
            x.pop("desc", "file_image")
            x.pop("file_html")
            self.__indexC.append(x["country"])

        return
    
    def downloadAll(self, savePath: str):
        for i in self.__countries:
            self.downloadOneCountry(savePath, id=i.get("id"))

        return

    def downloadOneCountry(self, savePath: str, id: str = 0, meta: str = "", country: str = "") -> bool:
        # Get id by country name
        if id == 0 and country in self.__indexC:
            meta = self.__countries[self.__indexC.index(country)]
            id = meta.get("id")
        elif id == 0 and country not in self.__indexC:
            print("Country {} is not collected in worlpop".format(country))
            return False
        
        url = "https://hub.worldpop.org/geodata/summary?id={}".format(id)
        super().__init__(url)
        r = self.rget()
        r.encoding = "utf-8"

        # Get all population file
        soup = bs(r.text, "html.parser")
        div = soup.find_all("div", {"id": "files"})
        a = div[0].find_all("a", {"class": "mt-3"})
        for i in a:
            downloadUrl = i["href"]
            iso = downloadUrl.split("/") # Format see in .downloadOneCountryByISO url
            filename = iso[-1]
            iso = iso[-3]
            savePath2 = os.path.join(savePath, iso)
            mkdir(savePath2)
            # Download
            super().__init__(downloadUrl)
            self.download(os.path.join(savePath2, filename), multi=False)
        
        # Save meta data
        pd.DataFrame(meta).to_csv(os.path.join(savePath2, "{}_metadata.csv".format(iso)), encoding="utf-8")

        return True

    def downloadOneCountryByISO(self, savePath, iso: str):
        url = "https://data.worldpop.org/GIS/AgeSex_structures/Global_2000_2020_Constrained_UNadj/2020/" \
            "{}//{}_{}_{}_2020_constrained_UNadj.tif"
        age = ["0", "1"] + [str(5 * x) for x in range(1, 17)]
        mkdir(savePath)
        for g in ["f", "m"]:
            for a in age:
                super().__init__(url.format(iso.upper(), iso, g, a))
                self.download(savePath, multi=False)

if __name__ == "__main__":
    a = globalPopulation()
    a.downloadOneCountry("test", country="Angola")