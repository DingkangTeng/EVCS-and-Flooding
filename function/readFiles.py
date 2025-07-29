import sys, os, json

sys.path.append(".") # Set path to the roots

# Folder operation function
## Make a floder
def mkdir(savePath: str) -> None:
    if not os.path.exists(savePath):
        os.mkdir(savePath)

    return

# Read files
class readFiles:
    __slots__ = ["path", "fileFilter", "typeFilter", "files"]

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
    
    def specificFloder(self, contains: list[str] = []) -> list[str]:
        allFolder = self.allFolder()
        result = set(allFolder)
        if contains != []:
            for contain in contains:
                result = result & set(x for x in allFolder if contain in x)
    
        return list(result)
    
    def specificFile(self, suffix: list[str] = [], contains: list[str] = []) -> list[str]:
        files = set(self.files)
        if suffix != []:
            files = files & set(x for x in files if x.split(".")[-1] in suffix)
        if contains != []:
            for contain in contains:
                files = files & set(x for x in files if contain in x)
                
        return list(files)

# Creat processed json record
class loadJsonRecord:
    @classmethod
    def load(cls, path: str, name: str, structure: list | dict = []) -> list | dict:
        if os.path.exists(path):
            with open(path, 'r') as f:
                j = json.load(f)
                if name in j.keys():
                    return j[name]
                else:
                    j[name] = structure
                    with open(path, 'w') as f:
                        json.dump(j, f, indent=4)
                    return structure
        else:
            with open(path, 'w') as f:
                json.dump({name: structure}, f, indent=4)
                return structure
            
    @classmethod
    def save(cls, path: str, name: str, result: list | dict) -> None:
        if os.path.exists(path):
            with open(path, 'r') as f:
                j = json.load(f)
                j[name] = result
                with open(path, 'w') as f:
                    json.dump(j, f, indent=4)
                return
        else:
            with open(path, 'w') as f:
                json.dump({name: result}, f, indent=4)
                return

# Debug
if __name__ == "__main__":
    # loadJsonRecord.load(r"C:\\0_PolyU\\roadsGraph\\updateStature2.json", "EVCS2")
    # loadJsonRecord.save(r"C:\\0_PolyU\\roadsGraph\\updateStature3.json", "EVCS3", ["a.d", "d.b"])
    print(readFiles(r"C:\\0_PolyU").specificFloder(contains=["population_"]))