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

import static com.dremio.plugins.elastic.ElasticsearchType.TEXT;

import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.service.namespace.SourceTableDefinition;

/* junit imports */
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Assume;

/* Test if hidden indices are queryable or not */
public class ITTestHiddenIndices extends ElasticBaseTestQuery {

  public static ElasticsearchStoragePlugin plugin;

  @BeforeClass
  public static void beforeStart() {
    Assume.assumeFalse(ElasticsearchCluster.USE_EXTERNAL_ES5);
  }


  @Before
  public void before() throws Exception {
    super.before();
    plugin = (ElasticsearchStoragePlugin) getSabotContext().getCatalogService().getSource("elasticsearch");
  }

  @Test
  public void testHiddenIndex() throws Exception {


      schema = ".hidden";

      ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
              new ElasticsearchCluster.ColumnData("location", TEXT, new Object[][]{
                      {"San Francisco"},
                      {"Oakland"},
                      {"San Jose"}
              })
      };

      //load up a hidden schema
      load(schema, table, data);

      schema = "nothidden";

      //load up a not hidden schema
      load(schema, table, data);

      //get all the datasets in the plugin
      Iterable<SourceTableDefinition> tables = plugin.getDatasets("",
          DatasetRetrievalOptions.IGNORE_AUTHZ_ERRORS
      );

      //make sure you can find the not hidden schema
      boolean foundNotHidden = false;

      //make sure you never find the hidden one
      for(SourceTableDefinition table : tables){
        String response = table.getName().getSchemaPath();
        assertTrue(!(response.contains(".hidden")));
        if(response.contains("nothidden")) {
          foundNotHidden = true;
        }
      }

      assertTrue(foundNotHidden);

  }
}
