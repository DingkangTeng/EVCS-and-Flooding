from osgeo import ogr

# Creat a new field in gpkg
def creatField(layer: ogr.Layer, fieldName: str, fieldType: int, maxLength: int = 255) -> None:
    """
    Create a new OGR field definition with the given name and OGR field type.
    fieldType should be one of the ogr.OFT* constants, e.g., ogr.OFTString, ogr.OFTInteger, etc.
    """
    fieldDefn = ogr.FieldDefn(fieldName, fieldType)
    if fieldType == ogr.OFTString:
        fieldDefn.SetWidth(max(80, min(maxLength, 255)))
    layer.CreateField(fieldDefn)

    return