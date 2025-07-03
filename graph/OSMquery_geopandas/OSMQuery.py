# -*- coding: utf-8 -*-
"""
/***************************************************************************
 OSMQuery geopandas modified version
 Based on A Python toolbox for ArcGIS named QSMQuery: https://github.com/riccardoklinger/OSMquery
 OSM Overpass API frontend
                             -------------------
        begin                : 2025-07-03
        copyright            : (C) 2025 by Dingkang Teng
        email                : dingkang.teng at connect.polyu dot hk
        contributors         : Dingkang Teng
 ***************************************************************************/
/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

import datetime, random, os, json
import geopandas as gpd
from urllib.request import Request, urlopen
from urllib.parse import quote
from typing import Optional
from shapely.geometry import Point, LineString, Polygon

from .dataStructure import *
from .config import *

# Constants for building the query to an Overpass API
QUERY_START = "[out:json][timeout:180]"
QUERY_DATE = '[date:"timestamp"];('
QUERY_END = ');(._;>;);out;>;'
GEOMETRY = {
    "POINT": Point,
    "POLYLINE": LineString,
    "LINE": LineString,
    "POLYGON": Polygon,
}

class Toolbox:
    @classmethod
    def get_server_URL(cls) -> str:
        """Load the configuration and find Overpass API endpoints"""
        return random.choice(OVERPASS_SERVERS)
    
    @classmethod
    def sanitize_field_name(cls, field_name: str) -> str:
        field_name = field_name.replace(":", "_").replace(".", "_").replace("-", "_")
        if field_name[0].isdigit():
            field_name = "_" + field_name
        return field_name

    @classmethod
    def extract_features_from_json(cls, data: dict) -> tuple[list[dict], list[dict], list[dict]]:
        """Extract lists of point, line, polygon objects from an Overpass API
        response JSON object"""
        points = [e for e in data['elements'] if e["type"] == "node"]
        lines = [e for e in data['elements'] if e["type"] == "way" and
                 (e["nodes"][0] != e["nodes"][len(e["nodes"])-1])]
        polygons = [e for e in data['elements'] if e["type"] == "way" and
                    (e["nodes"][0] == e["nodes"][len(e["nodes"])-1])]
        return points, lines, polygons

    @classmethod
    def get_attributes_from_features(cls, features) -> set[str]:
        fc_fields = set()
        for element in [e for e in features if "tags" in e]:
            for tag in element["tags"]:
                fc_fields.add(tag)
        return fc_fields
    
    @classmethod
    def convertJson(
        cls,
        vectorSet: tuple[list[dict], list[dict]],
        vectorType: str,
        attributes: set[str],
        requestTime: Optional[datetime.datetime],
        onlyGeo: bool = False
    ) -> dict[str, list]:
        
        addionalFields = {"geometry", "OSM_ID", "DATETIME"}
        # If only get geometric figure, other attribute will not be gethered
        if onlyGeo:
            attributes = set()
        result = {x:[] for x in attributes.union(addionalFields)}
        for element in vectorSet[0]:
            try:
                if vectorType == "POINT":
                    result["geometry"].append(Point(element["lon"],element["lat"]))
                    result["OSM_ID"].append(element["id"])
                    result["DATETIME"].append(requestTime)
                    for tag in attributes:
                            origionalTags = element["tags"]
                            processedTags = {Toolbox.sanitize_field_name(x): y for x, y in origionalTags.items()}
                            result[tag].append(processedTags.get(tag, None))
                else:
                    nodes = element["nodes"]
                    nodeGeometry = []
                    for node in nodes:
                            for NodeElement in vectorSet[1]:
                                if NodeElement["id"] == node:
                                    nodeGeometry.append((NodeElement["lon"], NodeElement["lat"]))
                                    break
                    if vectorType == "POLYGON":
                        # Polygon
                        result["geometry"].append(Polygon(nodeGeometry))
                        result["OSM_ID"].append(element["id"])
                        result["DATETIME"].append(requestTime)
                        # Now deal with the way tags:
                        for tag in attributes:
                            origionalTags = element["tags"]
                            processedTags = {Toolbox.sanitize_field_name(x): y for x, y in origionalTags.items()}
                            result[tag].append(processedTags.get(tag, None))
                    elif vectorType == "LINE":  # lines have different start end endnodes:
                        result["geometry"].append(LineString(nodeGeometry))
                        result["OSM_ID"].append(element["id"])
                        result["DATETIME"].append(requestTime)
                        # now deal with the way tags:
                        for tag in attributes:
                            origionalTags = element["tags"]
                            processedTags = {Toolbox.sanitize_field_name(x): y for x, y in origionalTags.items()}
                            result[tag].append(processedTags.get(tag, None))
                    else:
                        print("Wrong vector type")
                        break
            except:
                print("\nOSM element {} could not be written to FC".format(element["id"]))

        return result

    @classmethod
    def convertGdf(cls, data: list[dict], fcs: list[tuple[str, gpd.GeoDataFrame]]) -> None:
        j = 0
        NAME = ["point", "line", "polygon"]
        for i in data:
            if i != {}:
                fcs.append(
                    (
                        NAME[j],
                        gpd.GeoDataFrame(
                            i,
                            geometry=gpd.GeoSeries(i["geometry"]),
                            crs="EPSG:4326"
                            )
                    )

                )
            j += 1

    @classmethod
    def fill_feature_classes(
        cls,
        data: list[list[dict]],
        request_time: Optional[datetime.datetime],
        saveType: list[bool] = [True, True, True],
        onlyGeo: bool = False,
    ) -> list[gpd.GeoDataFrame]:
        # ------------------------------------------------------
        # Creating feature classes according to the response
        # ------------------------------------------------------

        # Extract geometries (if present) from JSON data: points (nodes),
        # lines (open ways; i.e. start and end node are not identical) and
        # polygons (closed ways)
        points = data[0]
        lines = data[1]
        polygons = data[2]

        # Per geometry type, gather all atributes present in the data
        # through elements per geometry type and collect their attributes
        # if user wants to get all the attributes
        # Per geometry type, create a feature class if there are features in
        # the data
        point_set = {}
        line_set = {}
        polygon_set = {}
        if len(points) > 0 and saveType[0]:
            point_fc_fields = Toolbox.get_attributes_from_features(points)
            point_set = Toolbox.convertJson((points, points), "POINT", point_fc_fields, request_time, onlyGeo)
        else:
            print("\nData contains no point features.")

        if len(lines) > 0 and saveType[1]:
            line_fc_fields = Toolbox.get_attributes_from_features(lines)
            line_set = Toolbox.convertJson((lines, points), "LINE", line_fc_fields, request_time, onlyGeo)
        else:
            print("\nData contains no line features.")

        if len(polygons) > 0 and saveType[2]:
            polygon_fc_fields = Toolbox.get_attributes_from_features(polygons)
            polygon_set = Toolbox.convertJson((polygons, points), "POLYGON", polygon_fc_fields, request_time, onlyGeo)
        else:
            print("\nData contains no polygon features.")

        result_fcs = []
        Toolbox.convertGdf([point_set, line_set, polygon_set], result_fcs)
        return result_fcs

    @classmethod
    def get_bounding_box(cls, extent_indication_method: str, region_name: str, extent: Optional[extent] = None) -> tuple[str, str]:
        """ Given a method for indicating the extent to be queried and either
        a region name or an extent object, construct the string with extent
        information for querying the Overpass API"""
        if extent_indication_method == "Define a bounding box" and extent is not None:
            bounding_box = [extent.YMin, extent.XMin, extent.YMax,
                                extent.XMax]
            return '', '(%s);' % ','.join(str(e) for e in bounding_box)

        elif extent_indication_method == "Geocode a region name":
            # Get an area ID from Nominatim geocoding service
            NOMINATIM_URL = 'https://nominatim.openstreetmap.org/search?q=' \
                            '%s&format=json' % quote(region_name)
            print("\nGecoding region using Nominatim:\n {}...".format(NOMINATIM_URL))
            q = Request(NOMINATIM_URL)
            q.add_header("User-Agent", "OSMquery/https://github.com/riccardoklinger/OSMquery")
            nominatim_response = urlopen(q)
            try:
                nominatim_data = json.loads(nominatim_response.read().decode("utf-8"))
                print(nominatim_data)
                nominatim_area_id = None
                for result in nominatim_data:
                    if result["osm_type"] == "relation":
                        nominatim_area_id = result['osm_id']
                        try:
                            print("\tFound region {}".format(result['display_name']))
                        except:
                            print("\tFound region {}".format(nominatim_area_id))
                        break
                if nominatim_area_id is not None:
                    bounding_box_head = "area({})->.searchArea;".format(int(nominatim_area_id) + 3600000000)
                    bounding_box_data = "(area.searchArea);"
                    return bounding_box_head, bounding_box_data
                else:
                    print("\tNo region found!")
                    return '', ''
            except:
                print("\tNo region found! Check region name or extent.")
                return '', ''
        else:
            raise ValueError


class GetOSMData:
    __slot__ = [
        "path", "OSMTagKey", "OSMTagValues", "method",
        "regionName", "extent", "crs", "referenceDate", "saveType"
    ]

    def __init__(
            self,
            OSMTagKey: str,
            OSMTagValues: list[str],
            method: str = "Define a bounding box",
            regionName: str = "",
            extent: Optional[extent] = None,
            onlyGeo: bool = False,
            referenceDate: Optional[datetime.datetime] = None,
            saveType: list[bool] = [True, True, True], # Save point, line, polygon data
        ):
        """
        Initialize the function

        Parameters:
        OSMTagKey: Choose the key of OSM tag.
        OSMTagValues: Chose the query values of OSM tag.
        method: There are two method, one is "Define a bounding box", when choosing this, \
            you should fill the parameter of extent. The other is "Geocode a region name", \
            when chhosing this, you should fill the parameter of regionName. (default: `"Define a bounding box"`)
        regionName: Using strings to describe a region, the tool will use geo-encoding to \
            find the best match place. (default: `""`)
        extent: Define a bounding box using rectangle with the data structure of \
            "extent(`X min`, `Y min`, `X max`, `Y max`)" basd on `WGS-84` coordinate system. (default: `None`)
        onlyGeo: When set True, the result will only output geometric vectors without attributes \
            fields. (default: `False`)
        referenceDate: Using datetime.datetime to determin the query time. (default: `None`).
        saveType: Using a list of bool to control the type of output resulte. The fist is to save points \
            results, the second is to save lines results, and the third is to save polygon resluts. \
            (default: `[True, True, True]`)

        Retruns:
        None
        """
        self.params = [ 
            OSMTagKey, #0
            OSMTagValues, #1
            method, #2
            regionName, #3
            extent, #4
            onlyGeo, #5
            None, #6 Not used
            referenceDate, #7
            saveType #8
        ]


    def execute(self, path: str = "", saveType: str = "GPKG", saveName: str = "result") -> None:
        """
        Get the OSM data

        Parameters:
        path: Save path of the result, the defalut path is the root path\
            of environment.
        saveType: Chose the driver of the saving function, support GPKG, \
            SHP and GEOJSON. The default value is GPKG. (default: `GPKG`)
        saveName: The name of the result file, the default name is "result".

        Retruns:
        None
        """
        params = self.params
        
        if params[7] is not None:
            query_date = QUERY_DATE.replace(
                "timestamp",
                params[7].strftime("%Y-%m-%dT%H:%M:%SZ")
            )
        else:
            query_date = ';('


        # Get the bounding box-related parts of the Overpass API query, using
        # the indicated extent or by geocoding a region name given by the user
        bbox_head, bbox_data = Toolbox.get_bounding_box(params[2], params[3], params[4])

        # Get the list of OSM tag values checked by the user. The tool makes
        # the user supply at least one key.
        tag_key = params[0]
        tag_values = params[1]

        # If the wildcard (*) option is selected, replace any other tag value
        # that might be selected
        if "'* (any value, including the ones listed below)'" in tag_values:
            print("\nCollecting {} = * (any value)".format(tag_key))
            node_data = 'node["' + tag_key + '"]'
            way_data = 'way["' + tag_key + '"]'
            relation_data = 'relation["' + tag_key + '"]'
        # Query only for one tag value
        elif len(tag_values) == 1:
            tag_value = tag_values[0]
            print("\nCollecting " + tag_key + " = " + tag_value)
            node_data = 'node["' + tag_key + '"="' + tag_value + '"]'
            way_data = 'way["' + tag_key + '"="' + tag_value + '"]'
            relation_data = 'relation["' + tag_key + '"="' + tag_value + '"]'
        # Query for a combination of tag values
        else:
            tag_values = "|".join(tag_values)
            print("\nCollecting " + tag_key + " = " + tag_values)
            tag_values = tag_values.replace("|", "$|^")
            tag_values = "^%s$" % tag_values
            node_data = 'node["' + tag_key + '"~"' + tag_values + '"]'
            way_data = 'way["' + tag_key + '"~"' + tag_values + '"]'
            relation_data = 'relation["' + tag_key + '"~"' + tag_values + '"]'

        query = (QUERY_START + query_date + bbox_head +
                 node_data + bbox_data +
                 way_data + bbox_data +
                 relation_data + bbox_data +
                 QUERY_END)

        print("Issuing Overpass API query: \n {}".format(query))
        # Get the server to use from the config:
        QUERY_URL = Toolbox.get_server_URL()
        print("Server used for the query: \n {}".format(QUERY_URL))
        q = Request(QUERY_URL)
        q.add_header("User-Agent", "OSMquery/https://github.com/riccardoklinger/OSMquery")
        response = urlopen(q, query.encode('utf-8'))

        #response = requests.get(QUERY_URL, params={'data': query})
        if response.getcode() != 200:
            print("\tOverpass server response was {}".format(response.getcode()))
            return
        try:
            data = json.loads(response.read())
        except:
            print("\tOverpass API responded with non JSON data: \n{}".format(response.read()))
            return
        if len(data["elements"]) == 0:
            print("\tNo data found!")
            return
        else:
            points, lines, polygons = Toolbox.extract_features_from_json(data)
            print("\nCollected {} point features (including reverse objects)".format(len(points)))
            print("Collected {} line features (including reverse objects)".format(len(lines)))
            print("Collected {} polygon features (including reverse objects)".format(len(polygons)))

            result_fcs = Toolbox.fill_feature_classes([points, lines, polygons], params[7], params[8], params[5])
        for i in result_fcs:
            if saveType == "GPKG":
                filename = saveName + ".gpkg"
                i[1].to_file(os.path.join(path, filename), layer=str(i[0]), driver="GPKG")
            elif saveType == "SHP":
                filename = "{}_{}.shp".format(saveName, i[1])
                i[1].to_file(os.path.join(path, filename), driver="ESRI Shapefile")
            elif saveType == "GEOJSON":
                filename = "{}_{}.geojson".format(saveName, i[1])
                i[1].to_file(os.path.join(path, filename), driver="GeoJSON")

        return