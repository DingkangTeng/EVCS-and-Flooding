import sqlite3
from typing import Any

FID_INDEX = "idx_fid"

# Load SpatiaLite extension
class spatialiteConnection(sqlite3.Connection):
    def loadSpatialite(self) -> bool:
        # Enable extension
        self.enable_load_extension(True)  
        err = []
        # Load extension
        spatialiteExtensions = [
            "mod_spatialite",  # Windows or some Linux, install the whole mod_spatialite-5.1.0-win-amd64 in C:\Windows\System on Windows
            "libspatialite",   # Other system
            "spatialite"       # Backup name
        ]
        loaded = False
        for extName in spatialiteExtensions:
            try:
                self.load_extension(extName)
                loaded = True
                break
            except sqlite3.OperationalError as e:
                err.append(e)
                continue

        if not loaded:
            errstr = "Failed to load SpatiaLite extension: \n"
            for e in err:
                errstr += (str(e) + "\n")
            raise RuntimeError(errstr + "Ensure it is installed.")
        
        return loaded
    
# Modify table
class modifyTable(sqlite3.Cursor):
    def addFields(self, tableName: str, *fields: tuple[str, str, Any]) -> None:
        self.execute("PRAGMA table_info({})".format(tableName))
        existingColumns = [col[1] for col in self.fetchall()]
        for field in fields:
            fieldName, colType, initialValue = field
            if fieldName not in existingColumns:
                if initialValue is not None:
                    self.execute(
                        f"""
                        ALTER TABLE {tableName}
                        ADD COLUMN {fieldName} {colType}
                        DEFAULT {initialValue}
                        """
                    )
                else:
                    self.execute(
                        f"""
                        ALTER TABLE {tableName}
                        ADD COLUMN {fieldName} {colType}
                        """
                    )
            self.execute(f"CREATE INDEX IF NOT EXISTS idx_{fieldName} ON {tableName} ({fieldName})")

        return