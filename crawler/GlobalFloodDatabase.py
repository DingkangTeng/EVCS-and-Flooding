import sys, os
from tqdm import tqdm

sys.path.append(".") # Set path to the roots

from crawler.crawler import crawler
from function.constant import COUNTRYS

class GlobalFloodDatabase(crawler):
    __url = "https://global-flood-database.cloudtostreet.ai/"

    def getAllCountry(self) -> None:
        """
        This function can only get js file from the websites at present.
        """
        super().__init__("{}/static/js/bundle.js".format(self.__url))
        r = self.rget()
        r.encoding = "utf-8"
        with open("function\\country.js", "wb") as file:
            file.write(r.content)

        return

    def downloadAll(self, savePath: str) -> None:
        for country in COUNTRYS:
            countryID = country["id"]
            print("Downloading {} ({}).".format(country["name"], countryID))
            self.downloadOneCountry(countryID, savePath)

        return

    def downloadOneCountry(self, country: str, savePath: str) -> None:
        """
        Download all flooding data in one country.

        Parameter: \n
        country: ISO 3166-1 country alpha-3 code.

        Return:
        No retrun.
        """
        downloadList = "https://global-flood-database.cloudtostreet.ai/collection/{}".format(country)
        savePath = os.path.join(savePath, country)
        os.makedirs(savePath) # Creat folder
        super().__init__(downloadList)
        r = self.rget()
        for date in r.json():
            path = date.split('/') #[projects/global-flood-db/gfd_v4/DFO_2060_From_20020921_to_20021008]
            url = "https://storage.googleapis.com/gfd_v1_4/{}.zip".format(path[3])
            path = os.path.join(savePath, path[3] + ".zip")
            super().__init__(url)
            self.download(path)
        
        return

if __name__ == "__main__":
    a = GlobalFloodDatabase()
    a.downloadAll("")
    # a.downloadOneCountry("AFG", "")
