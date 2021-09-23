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
    String sql = "SELECT flatten(kRingDistances(599686042433355775, 5)) AS ring FROM (VALUES(1))";

    QueryBuilder q = client.queryBuilder().sql(sql);
    RowSet results = q.rowSet();
    results.print();
  }
}
