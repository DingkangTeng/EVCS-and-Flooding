# OSMQuery with geopandas
This modification based on OSMQuery, a [Python Toolbox](https://pro.arcgis.com/en/pro-app/arcpy/geoprocessing_and_python/a-quick-tour-of-python-toolboxes.htm) for making it easy (easier) to get data out of [OpenStreetMap (OSM)](https://wiki.openstreetmap.org) and into the Esri ecosystem. With OSMQuery, you can query an area of interest for OSM data (of specified kind) and obtain feature layers of the results, with point, line and/or area features depending on what kind of data OSM holds for your area. This toolbox works both in ArcGIS Pro and in ArcGIS Desktop 10.x.

## Prerequisites
The plugin was developed using geopandas, do not need the basement of any edition of ArcGIS.
Besides arcpy, the plugin only uses core modules from Python3:

* sys
* datetime
* random
* os
* json
* geopandas
* urllib
* shapely

## More Details on Usage
### Querying OSM Tags
OSM tags like, for example, `amenity=bakery` consist of a key (in the example: `amenity`) and a value (in the example: `bakery`). In the `Get OSM Data` tool for simple queries, select a key and value(s) pair for which OSM should be queried for features. In each run of the tool, you can use only one key but you can use one or several values. For example, you can query only for `amenity=atm` or you can query for both `amenity=atm` and `amenity=bank` in one run of the tool. If you chose to do the latter, the results for different tags (or more specifically: OSM values) are summarized into one feature class per geometry type.

### Handling of Data Models (OSM Tag Key-Value Pairs)
The OSM tag keys become attribute names, the OSM tag values become attribute values. Specifically, in the case of querying multiple OSM tag values at once, e.g. `amenity=atm` and `amenity=bank`, your resulting feature layers will obtain the 'union' of the data models of the individual queries. In the example of `amenity=atm` and `amenity=bank` your resulting feature layer might have both the attributes `currencies` and `opening_hours`, where the former is only filled for ATMs and the latter is only filled for banks.

### Defining an Area of Interest
For defining the spatial extent of your query you can use two options: You can either enter a region name (which will be geocoded using the OSM-based geocoding service [Nominatim](https://nominatim.openstreetmap.org/search)) or you can define a bounding box using rectangle (`X min`, `Y min`, `X max`, `Y max`) basd on `WGS-84` coordinate system.

### Defining a Date and Time of Interest
Using the appropriate parameter you can set a reference date and time (the default is the current time), the tool will query OSM for the specified point in time and will only yield features that were part of OSM then. The reference date and time is given in [UTC (Coordinated Universal Time)](https://en.wikipedia.org/wiki/Coordinated_Universal_Time).

## Limitations
This tool in this toolbox rely on the [OSM Overpass API](https://wiki.openstreetmap.org/wiki/Overpass_API). As the size of an Overpass API query result is only known when the download of the resulting data is complete, it is not possible to give an estimate of the number of features a query yields, or the time-until-completion, during the query process. The Overpass API uses a timeout after which a query will not be completed. If you run into timeout problems consider narrowing your query thematically and/or spatially. For other limitations of the Overpass API and potential workarounds please consider the pertinent [OSM Wiki Page](https://wiki.openstreetmap.org/wiki/Overpass_API#Limitations).

## License and Credits
OSMQuery is licensed under the GNU General Public License (GPL), see [LICENSE](https://github.com/riccardoklinger/OSMquery/blob/master/LICENSE).

OSMQuery relies on the [OSM Overpass API](https://wiki.openstreetmap.org/wiki/Overpass_API) which is licensed under the [GNU Affero GPL v3](https://www.gnu.org/licenses/agpl-3.0.en.html). All OSM data you obtain through this tool are, of course, [&copy; OSM contributors](https://www.openstreetmap.org/copyright).
