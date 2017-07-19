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

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.plugins.elastic.ElasticBaseTestQuery.ElasticSSL;
import com.dremio.plugins.elastic.ElasticsearchCluster.ColumnData;

/**
 * Test that queries work when SSL is on
 */
@ElasticSSL(enabled=true)
public class TestSSL extends ElasticBaseTestQuery {

    @BeforeClass
    public static void beforeStart() {
      Assume.assumeFalse(ElasticsearchCluster.USE_EXTERNAL_ES5);
    }

    @Before
    public void loadTable() throws Exception {
      ColumnData[] data = getBusinessData();
      load(schema, table, data);
    }

    @After
    public void after() {
      elastic.wipe();
    }

    /**
     * Just testing that we can run a query when SSL is enabled
     * (see annotation on class)
     * @throws Exception
     */
    @Test
    public void test() throws Exception {
      String sqlQueryLimit1 = "select city from elasticsearch." + schema + "." + table + " limit 1";
      testBuilder().sqlQuery(sqlQueryLimit1).unOrdered()
          .baselineColumns("city")
          .baselineValues("Cambridge")
          .build()
          .run();
    }
}
