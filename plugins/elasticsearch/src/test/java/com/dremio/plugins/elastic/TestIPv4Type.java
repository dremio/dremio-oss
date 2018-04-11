/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static com.dremio.plugins.elastic.ElasticsearchType.IP;

import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.Text;
import org.junit.Test;


/**
 * Tests for elasticsearch IPv4 data type.
 */
public class TestIPv4Type extends ElasticBaseTestQuery {

  @Test
  public void testIPv4() throws Exception {

    String ip1 = "10.0.0.1";
    String ip2 = "192.168.0.1";

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("ip_field", IP, null,
                    new Object[][]{
                            {ip1}, {ip2}
                    })
    };

    elastic.load(schema, table, data);


    testBuilder()
            .sqlQuery("select ip_field from elasticsearch." + schema + "." + table)
            .baselineColumns("ip_field")
            .unOrdered()
            .baselineValues(ip1)
            .baselineValues(ip2)
            .go();

    testBuilder()
        .sqlQuery("select ip_field from elasticsearch." + schema + "." + table + " group by ip_field")
        .baselineColumns("ip_field")
        .unOrdered()
        .baselineValues(ip1)
        .baselineValues(ip2)
        .go();
  }

  @Test
  public void testArrayOfIPv4() throws Exception {

    String ip1 = "10.0.0.1";
    String ip2 = "192.168.0.1";
    String ip3 = "10.0.8.6";
    String ip4 = "10.0.8.5";

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
            new ElasticsearchCluster.ColumnData("ip_field", IP, null,
                    new Object[][]{
                            new Object[]{ip1, ip2, ip3},
                            new Object[]{ip4, ip1}
                    })
    };

    elastic.load(schema, table, data);


    JsonStringArrayList<Text> values1 = new JsonStringArrayList<>();
    JsonStringArrayList<Text> values2 = new JsonStringArrayList<>();

    values1.add(new Text(ip1));
    values1.add(new Text(ip2));
    values1.add(new Text(ip3));

    values2.add(new Text(ip4));
    values2.add(new Text(ip1));

    testBuilder()
            .sqlQuery("select ip_field from elasticsearch." + schema + "." + table)
            .baselineColumns("ip_field")
            .unOrdered()
            .baselineValues(values1)
            .baselineValues(values2)
            .go();
  }

  @Test
  public void testIPv4AggregateMin() throws Exception {

    String ip1 = "10.0.0.1";
    String ip2 = "192.168.0.1";

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("ip_field", IP, null,
            new Object[][]{
                {ip1}, {ip2}
            })
    };

    elastic.load(schema, table, data);

    testBuilder()
        .sqlQuery("select min(ip_field) from elasticsearch." + schema + "." + table)
        .baselineColumns("EXPR$0")
        .unOrdered()
        .baselineValues(ip1)
        .go();
  }

  @Test
  public void testIPv4AggregateGroupBy() throws Exception {

    String ip1 = "10.0.0.1";
    String ip2 = "192.168.0.1";
    String ip3 = "10.0.0.1";

    ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
        new ElasticsearchCluster.ColumnData("ip_field", IP, null,
            new Object[][]{
                {ip1}, {ip2}, {ip3}
            })
    };

    elastic.load(schema, table, data);

    testBuilder()
        .sqlQuery("select ip_field from elasticsearch." + schema + "." + table + " group by ip_field")
        .baselineColumns("ip_field")
        .unOrdered()
        .baselineValues(ip1)
        .baselineValues(ip2)
        .go();
  }


}
