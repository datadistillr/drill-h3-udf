# Drill H3 Utility Functions
H3 is a geospatial indexing system using a hexagonal grid that can be (approximately) subdivided into finer and finer hexagonal grids, combining the benefits of a hexagonal grid with S2's hierarchical subdivisions.

Documentation is available at https://h3geo.org/. Developer documentation in Markdown format is available under the dev-docs directory.

All Drill functions can accept h3 either as a `BIGINT` or a `VARCHAR`.  The functions will return the same data type as the input. 


## Functions

### Indexing Functions
* `geoToH3(<latitude>, <longitude>, <resolution>)`: Indexes the location at the specified resolution, returning the index of the cell containing the location.  Returns 0 on 
  error.  Latitude and longitude are doubles and resolution is an int, returns a `BIGINT`.

* `geoToH3Address(<latitude>, <longitude>, <resolution>)`: Indexes the location at the specified resolution, returning the index of the cell containing the location.  Returns 0 on 
  error.  Latitude and longitude are doubles and resolution is an `int`, returns a `VARCHAR`.
* `h3ToGeoPoint(<h3 geo point>)`: Gets the centroid of an index.  Returns as `ST_Point`.

### Inspection Functions

* `h3GetResolution(<h3>)`: Returns the resolution of the index.
* `h3GetBaseCell(<h3>)`: Returns the base cell number of the index.
* `stringToH3(<h3Address)`: Converts the string representation to H3Index (uint64_t) representation.  Returns 0 on error.
* `h3ToString(<h3>)`: Converts the H3Index representation of the index to the string representation. str must be at least of length 17.
* `h3IsValid(<h3>)`: Returns non-zero if this is a valid H3 index.
* `h3IsResClassIII(<h3>)`: Returns non-zero if this index has a resolution with Class III orientation.
* `h3IsPentagon(<h3>`: Returns non-zero if this index represents a pentagonal cell.


### Traversal Functions

* `kRing(<origin h3>, <k>)`: k-rings produces indices within k distance of the origin index. k-ring 0 is defined as the origin index, k-ring 1 is defined as k-ring 0 and all 
  neighboring indices, and so on. Output is placed in the provided array in no particular order. Elements of the output array may be left zero, as can happen when crossing a pentagon.
* `kRingDistances(<origin h3>, <k>)`: k-rings produces indices within k distance of the origin index. k-ring 0 is defined as the origin index, k-ring 1 is defined as k-ring 0 and all neighboring indices, and so on. Output is placed in the provided array in no particular order. Elements of the output array may be left zero, as can happen when crossing a pentagon.
* `hexRing(<h3>, <k>)`: Produces the hollow hexagonal ring centered at origin with sides of length k. Returns 0 if no pentagonal distortion was encountered.
* `h3Line(<h3 start>, <h3 end>)`: Given two H3 indexes, return the line of indexes between them (inclusive).  This function may fail to find the line between two indexes, for 
  example if they are very far apart. It may also fail when finding distances for indexes on opposite sides of a pentagon.
* `h3Distance(<a>, <b>)`:  Returns the distance in grid cells between the two indexes. Returns a negative number if finding the distance failed. Finding the distance can fail 
  because the two indexes are not comparable (different resolutions), too far apart, or are separated by pentagonal distortion. This is the same set of limitations as the local IJ coordinate space functions.