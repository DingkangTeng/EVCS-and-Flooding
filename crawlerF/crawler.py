from __future__ import annotations

import requests, time, random, multitasking, sys
from tqdm import tqdm
from retry import retry

sys.path.append(".") # Set path to the roots
from crawlerF.apiKey import HEADERS, MB

class crawler:
    __slots__ = ["url", "postData"]
    __headers = HEADERS

    def __init__(self, url: str, postData: dict = {}):
        self.url = url
        self.postData = postData

    def rpost(self) -> requests.Response:
        while True:
            try:
                r = requests.post(self.url, headers=self.__headers, data = self.postData, timeout=(3,7))
                if self.__staureCode(r, "post"):
                    time.sleep(random.randint(5,10))
                    continue
                else:
                    return r
            except:
                print("Network Error")
                time.sleep(random.randint(5,10))
                continue
    
    def rget(self, stream=False) -> requests.Response:
        while True:
            try:
                r = requests.get(self.url, headers=self.__headers, timeout=(3,7), stream=stream)
                if self.__staureCode(r, "get"):
                    time.sleep(random.randint(5,10))
                    continue
                else:
                    return r
            except:
                print("Network Error")
                time.sleep(random.randint(5,10))
                continue
    
    def head(self)  -> requests.Response:
        while True:
            try:
                r = requests.head(self.url, headers=self.__headers, timeout=(3,7))
                if self.__staureCode(r, "geting head"):
                    time.sleep(random.randint(5,10))
                    continue
                else:
                    return r
            except:
                print("Network Error")
                time.sleep(random.randint(5,10))
                continue
        
    def download(self, savePath: str, retryTimes: int = 3, eachSize: int = 16*MB, multi: bool = True) -> None:
        """
        Multitask Split downloading function

        Parameters: \n
        savePath: File saving path. \n
        retryTimes: Retry time of the establishment of downloading connection. \n
        eachSize: Size of split task.

        Return:
        No return
        """
        file = open(savePath, "wb")
        fileSize = self.__getFileSize()

        @retry(tries=retryTimes)
        @multitasking.task
        def startDownload(start: int, end: int) -> None:
            """
            Download files based on their starting and ending positions

            Parameters: \n
            start : Starting position. \n
            end : Ending position.

            Retrun:
            No retrun.
            """
            _headers = HEADERS.copy()
            # Core of split download
            _headers['Range'] = f'bytes={start}-{end}'
            # Initiating a stream request
            response = session.get(self.url, headers=_headers, stream=True)
            # The size of the streaming response read each time
            chunkSize = 128
            # Temporarily store the obtained response and write it in a loop afterwards
            chunks = []
            for chunk in response.iter_content(chunk_size=chunkSize):
                chunks.append(chunk)
                # Update progress bar
                bar.update(chunkSize)
            file.seek(start)
            for chunk in chunks:
                file.write(chunk)
            # Release sources
            del chunks

        # Whether use mutithreads
        session = requests.Session()
        if multi and fileSize != 0:
            eachSize = min(eachSize, fileSize)
        else:
            eachSize = fileSize

        # Split
        parts = self.__split(0, fileSize, eachSize)
        # Progress bar
        bar = tqdm(total=fileSize, desc="Downloading: {}".format(self.url))
        for part in parts:
            start, end = part
            startDownload(start, end)
        # Waiting multitask ends
        multitasking.wait_for_tasks()
        file.close()
        bar.close()

        return
    
    # Split downloading
    @staticmethod
    def __split(start: int, end: int, step: int) -> list[tuple[int, int]]:
        parts = [(start, min(start+step, end))
                for start in range(0, end, step)]
        
        return parts
    
    # Get file size
    def __getFileSize(self, raiseError: bool = False) -> int:
        """
        Get file Size

        Parameters: \n
        raiseError : Wheter raise error when file size is not availabel.

        Return:
        File size (Bit)
        """
        h = self.head()
        fileSize = h.headers.get('Content-Length')
        if fileSize is None:
            if raiseError is True:
                raise ValueError("Do not support downloading!")
            return 0
        
        return int(fileSize)

    @staticmethod
    def __staureCode(r: requests.Response, rtype: str) -> bool:
        code = r.status_code
        if code != 200:
            print("Error, stature code in {} is {}.".format(rtype, code))
            time.sleep(random.randint(5,10))
            return True
        else:
            return False
        
# Debug
if "__main__" == __name__:
    # url = 'https://mirrors.tuna.tsinghua.edu.cn/pypi/web/packages/0d/ea/f936c14b6e886221e53354e1992d0c4e0eb9566fcc70201047bb664ce777/tensorflow-2.3.1-cp37-cp37m-macosx_10_9_x86_64.whl#sha256=1f72edee9d2e8861edbb9e082608fd21de7113580b3fdaa4e194b472c2e196d0'
    url = 'https://issuecdn.baidupcs.com/issue/netdisk/yunguanjia/BaiduNetdisk_7.2.8.9.exe'
    file_name = 'BaiduNetdisk_7.2.8.9.exe'
    # Satrt Downloading
    crawler(url).download(file_name)