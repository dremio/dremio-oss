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
package com.dremio.plugins.elastic;

import static com.dremio.plugins.elastic.ElasticsearchType.STRING;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.types.TypeProtos.DataMode;
import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.sabot.rpc.user.QueryDataBatch;

public class TestLimit0 extends ElasticBaseTestQuery {

  @Test
  public void test() throws Exception {

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("column", STRING, new Object[][]{
                    {"value"}
            })
    };

    elastic.load(schema, table, data);

    List<QueryDataBatch> results = testSqlWithResults(String.format("select * from elasticsearch.%s.%s limit 0", schema, table));

    MajorType t = results.get(0).getHeader().getDef().getField(0).getMajorType();
    Assert.assertEquals("Mismatched type", MinorType.VARCHAR, t.getMinorType());
    Assert.assertEquals("Mismatched datamode", DataMode.OPTIONAL, t.getMode());
  }
}
