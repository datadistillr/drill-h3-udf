/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datadistillr.udf;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.drill.test.rowSet.RowSetUtilities.longArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;


public class H3TraversalUDFTest extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testKRing() throws Exception {
    String sql = "SELECT flatten(kRing(599686042433355775, 5)) AS ring FROM (VALUES(1)) LIMIT 5";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("ring", MinorType.BIGINT)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow(599686042433355775L)
      .addRow(599686030622195711L)
      .addRow(599686044580839423L)
      .addRow(599686038138388479L)
      .addRow(599686043507097599L)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testStringKRing() throws Exception {
    String sql = "SELECT flatten(kRing('8928308280fffff', 5)) AS ring FROM (VALUES(1)) LIMIT 5";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("ring", MinorType.VARCHAR)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow("8928308280fffff")
      .addRow("8928308280bffff")
      .addRow("89283082873ffff")
      .addRow("89283082877ffff")
      .addRow("8928308283bffff")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testKRingDistances() throws Exception {
    String sql = "SELECT flatten(kRingDistances(599686042433355775, 5)) AS ring FROM (VALUES(1)) LIMIT 3";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addArray("ring", MinorType.BIGINT)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow((Object) longArray(599686042433355775L))
      .addRow((Object) longArray(599686030622195711L, 599686044580839423L, 599686038138388479L, 599686043507097599L, 599686015589810175L, 599686014516068351L))
      .addRow((Object) longArray(599686034917163007L, 599686029548453887L, 599686032769679359L, 599686198125920255L, 599686040285872127L, 599686041359613951L, 599686039212130303L,
        599686023106002943L, 599686027400970239L, 599686013442326527L, 599686012368584703L, 599686018811035647L))
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testKRingStringDistances() throws Exception {
    String sql = "SELECT flatten(kRingDistances('8928308280fffff', 5)) AS ring FROM (VALUES(1)) LIMIT 3";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addArray("ring", MinorType.VARCHAR)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow((Object) strArray("8928308280fffff"))
      .addRow((Object) strArray("8928308280bffff", "89283082873ffff", "89283082877ffff", "8928308283bffff", "89283082807ffff", "89283082803ffff"))
      .addRow((Object) strArray("8928308281bffff", "89283082857ffff", "89283082847ffff", "8928308287bffff", "89283082863ffff", "89283082867ffff", "8928308282bffff",
        "89283082823ffff", "89283082833ffff", "892830828abffff", "89283082817ffff", "89283082813ffff"))
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testHexRangeStringDistances() throws Exception {
    String sql = "SELECT flatten(hexRange('8928308280fffff', 1)) AS ring FROM (VALUES(1)) LIMIT 3";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addArray("ring", MinorType.VARCHAR)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow((Object) strArray("8928308280fffff"))
      .addRow((Object) strArray("8928308280bffff", "89283082873ffff", "89283082877ffff", "8928308283bffff", "89283082807ffff", "89283082803ffff"))
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testH3Line() throws Exception {
    String sql = "SELECT h3Line(599686042433355775, 599686023106002943) as line FROM (VALUES(1))";
    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addArray("line", MinorType.BIGINT)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow((Object) longArray(599686042433355775L, 599686043507097599L, 599686023106002943L))
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testH3StringLine() throws Exception {
    String sql = "SELECT h3Line('85283473fffffff', '8528342bfffffff') as line FROM (VALUES(1))";
    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addArray("line", MinorType.VARCHAR)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow((Object) strArray("85283473fffffff","85283477fffffff","8528342bfffffff"))
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testH3Distance() throws Exception {
    String sql = "SELECT h3Distance(599686042433355775, 599686023106002943) as distance FROM (VALUES(1))";
    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("distance", MinorType.BIGINT)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow(2)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testH3StringDistance() throws Exception {
    String sql = "SELECT h3Distance('85283473fffffff', '8528342bfffffff') as distance FROM (VALUES(1))";
    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("distance", MinorType.BIGINT)
      .build();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow(2)
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
