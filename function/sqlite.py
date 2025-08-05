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
    def addFields(self, tableName: str, *fields: tuple[str, str, Any, bool]) -> None:
        """
        fields: field name, field type, initial value, wheter index
        """
        self.execute("PRAGMA table_info({})".format(tableName))
        existingColumns = [col[1] for col in self.fetchall()]
        for field in fields:
            fieldName, colType, initialValue, whetherIndex = field
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
                if whetherIndex:
                    self.addIndex(fieldName, tableName)

        return
    
    def addIndex(self, fieldName: str, tableName: str) -> None:
        self.execute(f"CREATE INDEX IF NOT EXISTS idx_{fieldName} ON {tableName} ({fieldName})")

        return
    
    def dropFields(self, tableName: str, *fieldNames: str) -> None:
        cursor.execute(f"PRAGMA table_info({tableName})")
        columns = [row[1] for row in cursor.fetchall()]
        cursor.execute(f"PRAGMA index_list({tableName})")
        indexes = [row[1] for row in cursor.fetchall()]
        for fieldName in fieldNames:
            # Del index if exists
            relatedIndexes = []
            for index in indexes:
                cursor.execute(f"PRAGMA index_info({index})")
                indexColumns = [row[2] for row in cursor.fetchall()]
                if fieldName in indexColumns:
                    relatedIndexes.append(index)
            for index in relatedIndexes:
                cursor.execute(f"DROP INDEX {index}")
            # Del column
            if fieldName in columns:
                self.execute(
                    f"""
                    ALTER TABLE {tableName}
                    DROP COLUMN {fieldName}
                    """
                )

        return
    
if __name__ == "__main__":
    conn = sqlite3.connect("test\\CHN.gpkg", factory=spatialiteConnection)
    conn.loadSpatialite() # Load spatialite extension
    cursor = conn.cursor(factory=modifyTable)
    cursor.dropFields("nodes", "R", "A")
    conn.commit()
    conn.close()