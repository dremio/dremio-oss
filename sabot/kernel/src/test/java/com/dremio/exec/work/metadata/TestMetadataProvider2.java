/*
 * Copyright (C) 2017 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.work.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.proto.UserProtos.ColumnMetadata;
import com.dremio.exec.proto.UserProtos.GetColumnsResp;
import com.dremio.exec.proto.UserProtos.RequestStatus;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;

public class TestMetadataProvider2 extends BaseTestQuery {

  @Test
  public void columns() throws Exception {

    final GetColumnsResp resp1 = client.getColumns(null, null, null, null).get();
    assertEquals(RequestStatus.OK, resp1.getStatus());

    final List<ColumnMetadata> columns1 = resp1.getColumnsList();
    assertEquals(121, columns1.size());
    assertTrue("incremental update column shouldn't be returned",
        FluentIterable.from(columns1)
            .filter(new Predicate<ColumnMetadata>() {
              @Override
              public boolean apply(ColumnMetadata input) {
                return input.getColumnName()
                    .equals(IncrementalUpdateUtils.UPDATE_COLUMN);
              }
            }).isEmpty());

    test("SELECT * from cp.`tpch/customer.parquet`");
    // 8 columns from cp.`tpch/customer.parquet` table are now available from info schema

    final GetColumnsResp resp2 = client.getColumns(null, null, null, null).get();
    assertEquals(RequestStatus.OK, resp2.getStatus());

    final List<ColumnMetadata> columns2 = resp2.getColumnsList();
    assertEquals(129, columns2.size());
    assertTrue("incremental update column shouldn't be returned",
        FluentIterable.from(columns2)
            .filter(new Predicate<ColumnMetadata>() {
              @Override
              public boolean apply(ColumnMetadata input) {
                return input.getColumnName()
                    .equals(IncrementalUpdateUtils.UPDATE_COLUMN);
              }
            }).isEmpty());
  }

}
