# Drill H3 Utility Functions
H3 is a geospatial indexing system using a hexagonal grid that can be (approximately) subdivided into finer and finer hexagonal grids, combining the benefits of a hexagonal grid with S2's hierarchical subdivisions.

Documentation is available at https://h3geo.org/. Developer documentation in Markdown format is available under the dev-docs directory.

## Functions

### Indexing Functions
* `geoToH3(<latitude>, <longitude>, <resolution>)`: Indexes the location at the specified resolution, returning the index of the cell containing the location.  Returns 0 on 
  error.  Latitude and longitude are doubles and resolution is an int, returns a `BIGINT`.

* `geoToH3Address(<latitude>, <longitude>, <resolution>)`: Indexes the location at the specified resolution, returning the index of the cell containing the location.  Returns 0 on 
  error.  Latitude and longitude are doubles and resolution is an `int`, returns a `VARCHAR`.
* `h3ToGeoPoint(<h3 geo point>)`: Gets the centroid of an index.  Returns as `ST_Point`.
* 
