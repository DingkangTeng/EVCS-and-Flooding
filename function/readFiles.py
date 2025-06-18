import sys, os

sys.path.append(".") # Set path to the roots

# Folder operation function
## Make a floder
def mkdir(savePath: str) -> None:
    if not os.path.exists(savePath):
        os.mkdir(savePath)

    return

# Read files
class readFiles:
    __slot__ = ["path", "fileFilter", "typeFilter"]

    def __init__(self, path: str = "", fileFilter: list[str] = [], typeFilter: list[str] = []):
        self.path = path
        self.files = os.listdir(path)
        typeFilter += ["py"]
        for i in range(len(self.files) - 1, -1, -1):
            file = self.files[i]
            if file.split(".")[-1] in typeFilter or file in fileFilter:
                self.files.pop(i)
    
    def allFolder(self) -> list[str]:
        return [x for x in self.files if len(x.split(".")) == 1]
    
    def specifcFile(self, suffix: list[str] = []) -> list[str]:
        files = self.files
        if suffix != []:
            files = [x for x in files if x.split(".")[-1] in suffix]
        return files