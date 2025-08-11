import sys, os, json
from typing import Iterator

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
    __slots__ = ["path", "name", "result"]

    def __init__(self, path: str, name: str, structure: list[str] | dict[str, list[str]] = []):
        self.path = path
        self.name = name
        self.result = structure
        if os.path.exists(path):
            with open(path, 'r') as f:
                j = json.load(f)
                if name in j.keys():
                    self.result = j[name]
                    return
                else:
                    j[name] = structure
                    with open(path, 'w') as f:
                        json.dump(j, f, indent=4)
                    return
        else:
            with open(path, 'w') as f:
                json.dump({name: structure}, f, indent=4)
                return
    
    def __iter__(self) -> Iterator:
        return iter(self.result)
    
    def __len__(self) -> int:
        return len(self.result)
    
    def __str__(self) -> str:
        return str(self.result)
    
    def get(self, key: str, default: list = []) -> list[str]:
        if isinstance(self.result, list):
            return self.result
        else:
            value = self.result.get(key, default)
            if isinstance(value, list):
                return value
            elif isinstance(value, str):
                return [value]
            elif value is None:
                return default
            else:
                return list(value)
    
    # add update data
    def append(self, item: str | dict[str, list]) -> None:
        if isinstance(self.result, list) and isinstance(item, str):
            self.result.append(item)
        elif isinstance(self.result, dict) and isinstance(item, dict):
            key, result= next(iter(item.items()))
            self.result[key] = result

    def save(self) -> None:
        if os.path.exists(self.path):
            with open(self.path, 'r') as f:
                j = json.load(f)
                j[self.name] = self.result
                with open(self.path, 'w') as f:
                    json.dump(j, f, indent=4)
                return
        else:
            with open(self.path, 'w') as f:
                json.dump({self.name: self.result}, f, indent=4)
                return

# Debug
if __name__ == "__main__":
    # loadJsonRecord.load(r"C:\\0_PolyU\\roadsGraph\\updateStature2.json", "EVCS2")
    # loadJsonRecord.save(r"C:\\0_PolyU\\roadsGraph\\updateStature3.json", "EVCS3", ["a.d", "d.b"])
    print(readFiles(r"C:\\0_PolyU").specificFloder(contains=["population_"]))