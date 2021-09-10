package com.datadistillr.udf;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;


public class H3IndexingUDFTest extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testGeoToH3() throws RpcException {
    String sql = "SELECT geoToH3(37.775938728915946, -122.41795063018799, 9) AS address " +
      "FROM (VALUES(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("address", MinorType.BIGINT)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow(617700169958293503L)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testGeoToH3Address() throws RpcException {
    String sql = "SELECT geoToH3Address(37.775938728915946, -122.41795063018799, 9) AS address " +
      "FROM (VALUES(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("address", MinorType.VARCHAR)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow("8928308280fffff")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testH3ToGeoPoint() throws RpcException {
    String sql = "SELECT geo_data.geo_point.latitude AS latitude, geo_data.geo_point.longitude AS longitude FROM (SELECT h3ToGeo(617700169958293503) AS geo_point " +
      "FROM (VALUES(1))) AS geo_data";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("latitude", MinorType.FLOAT8, DataMode.OPTIONAL)
      .add("longitude", MinorType.FLOAT8, DataMode.OPTIONAL)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow(37.77670234943567, -122.41845932318311)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testH3ToGeoPointFromString() throws RpcException {
    String sql = "SELECT geo_data.geo_point.latitude AS latitude, geo_data.geo_point.longitude AS longitude FROM (SELECT h3ToGeo('8928308280fffff') AS geo_point " +
      "FROM (VALUES(1))) AS geo_data";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("latitude", MinorType.FLOAT8, DataMode.OPTIONAL)
      .add("longitude", MinorType.FLOAT8, DataMode.OPTIONAL)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow(37.77670234943567, -122.41845932318311)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
