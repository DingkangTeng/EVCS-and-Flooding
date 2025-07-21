# It seems that popWorld do not support mutlti-thread downloading?
import sys, os
import pandas as pd
from bs4 import BeautifulSoup as bs
from bs4 import Tag

sys.path.append(".") # Set path to the roots

from crawlerF.crawler import crawler
from function.readFiles import mkdir

class globalPopulation(crawler):
    __url = "https://hub.worldpop.org/ajax/geolisting/category?id=88"
    __countries = []
    """
    meta example:
    [{'id': '50353',
        'doi': '10.5258/SOTON/WP00698',
        'popyear': '2020',
        'date': '2020-11-30',
        'file_img': 'dza_f_45_2020_Image.png',
        'continent': 'Africa',
        'country': 'Algeria',
        'resolution': '100',
        'type': 'Age structures'}]
    """
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

    def downloadOneCountry(self, savePath: str, id: str = "", country: str = "") -> bool:
        # Get id by country name
        meta = {}
        if id == "" and country in self.__indexC:
            meta: dict = self.__countries[self.__indexC.index(country)]
            id = meta["id"]
        elif id == "" and country not in self.__indexC:
            print("Country {} is not collected in worlpop".format(country))
            return False
        
        url = "https://hub.worldpop.org/geodata/summary?id={}".format(id)
        super().__init__(url)
        r = self.rget()
        r.encoding = "utf-8"

        # Get all population file
        soup = bs(r.text, "html.parser")
        div = soup.find_all("div", {"id": "files"})
        da = div[0]
        if not isinstance(da, Tag):
            print("No data found for country {}".format(country))
            return False
        a = da.find_all("a", {"class": "mt-3"})
        # Add the folder of country
        db = a[0]
        if not isinstance(db, Tag):
            print("No data found for country {}".format(country))
            return False
        downloadUrl0 = db["href"]
        if downloadUrl0 is not str:
            print("No data found for country {}".format(country))
            return False
        iso = downloadUrl0.split("/") # Format see in .downloadOneCountryByISO url
        iso = iso[-3]
        savePath2 = os.path.join(savePath, iso)
        mkdir(savePath2)
        for i in a:
            if not isinstance(i, Tag):
                continue
            downloadUrl = i["href"]
            if downloadUrl is not str:
                continue
            filename = downloadUrl.split("/")[-1]
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
    for country in ["IDN", "IND", "IRN", "IRQ", "ISR", "JOR", "JPN", "KAZ", "KGZ", "KWT", "LAO", "LBN"]:
        a.downloadOneCountry("test", country=country)