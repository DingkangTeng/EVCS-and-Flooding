# It seems that popWorld do not support mutlti-thread downloading?
import sys, os
import pandas as pd
from bs4 import BeautifulSoup as bs
from bs4 import Tag
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

sys.path.append(".") # Set path to the roots

from crawler.basicCrawler import crawler
from _function.readFiles import mkdir, readFiles

class globalPopulation:
    # __url = "https://hub.worldpop.org/ajax/geolisting/category?id=88"
    __url = "https://hub.worldpop.org/ajax/geolisting/category?id=138" #2025 data
    __countries: list[dict[str, str]] = []
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
    
    def getAllCountries(self, year: int = 2025) -> None:
        tqdm.write("Getting all countries' metadata from WolfPop...")
        r = crawler(self.__url).rget()
        j = r.json()
        bar = tqdm(total=len(j), desc="Getting all countries' metadata from WolfPop", unit="country")
        for x in j:
            bar.update()
            if x["popyear"] != str(year): continue
            x.pop("desc", "file_image")
            x.pop("file_html")
            self.__indexC.append(x["country"])
            self.__countries.append(x)

        bar.close()

        return
    
    def downloadAll(self, savePath: str, maxThread: int = 1):
        futures = []
        futureDict = {}
        with ProcessPoolExecutor(max_workers=maxThread) as executor:
            for i in self.__countries:
                future = executor.submit(self.downloadOneCountry, savePath, id=i["id"])
                futures.append(future)
                futureDict[future] = i["country"]

            for future in as_completed(futures):
                c = futureDict[future]
                try:
                    future.result()
                except Exception as e:
                    tqdm.write(f"{c}: {e}")

        return

    def downloadOneCountry(self, savePath: str, id: str = "", country: str = "") -> bool:
        # Get id by country name
        meta = {}
        if id == "" and country in self.__indexC:
            meta: dict = self.__countries[self.__indexC.index(country)]
            id = meta["id"]
            country = meta["country"]
        elif id == "" and country not in self.__indexC:
            print("Country {} is not collected in worlpop".format(country))
            return False
        
        url = "https://hub.worldpop.org/geodata/summary?id={}".format(id)
        r = crawler(url).rget()
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
        if not isinstance(downloadUrl0, str):
            print("No data found for country {}".format(country))
            return False
        iso = downloadUrl0.split("/") # Format see in .downloadOneCountryByISO url
        iso = iso[-5] # Old version iso = iso[-3]
        savePath2 = os.path.join(savePath, iso)
        mkdir(savePath2)
        existFile = readFiles(savePath2).specificFile(suffix=["tif"])
        for i in a:
            if not isinstance(i, Tag):
                continue
            downloadUrl = i["href"]
            if not isinstance(downloadUrl, str): continue
            # if str(year) != downloadUrl.split("/")[7]: continue
            filename = downloadUrl.split("/")[-1]
            if filename in existFile:
                if os.path.getsize(os.path.join(savePath2, filename)) == 0: continue
                else: os.remove(os.path.join(savePath2, filename))
            # Download
            crawler(downloadUrl).download(os.path.join(savePath2, filename), multi=False)
        
        # Save meta data
        pd.DataFrame(meta).to_csv(os.path.join(savePath2, "{}_metadata.csv".format(iso)), encoding="utf-8")

        return True

    def downloadOneCountryByISO(self, savePath: str, iso: str):
        fileName = "{}_{}_{}_2020_constrained_UNadj.tif"
        url = "https://data.worldpop.org/GIS/AgeSex_structures/Global_2000_2020_Constrained_UNadj/2020/{}//{}"
        age = ["0", "1"] + [str(5 * x) for x in range(1, 17)]
        mkdir(savePath)
        existFile = readFiles(savePath).specificFile(suffix=["tif"])
        for g in ["f", "m"]:
            for a in age:
                file = fileName.format(iso.lower(), g, a)
                if file in existFile:
                    continue
                crawler(url.format(iso.upper(), file)).download(os.path.join(savePath, file), multi=False)

if __name__ == "__main__":
    DOWN_POP = os.path.join("..", "_Data", "globalPopulation")
    a = globalPopulation()
    # for country in ["USA"]:
    #     a.downloadOneCountryByISO(os.path.join("C:\\0_PolyU\\population2_tmp", country), country)
    a.downloadAll(DOWN_POP, 24)