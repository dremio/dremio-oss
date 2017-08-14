/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic;

import static com.dremio.plugins.elastic.ElasticBaseTestQuery.TestNameGenerator.aliasName;
import static com.dremio.plugins.elastic.ElasticBaseTestQuery.TestNameGenerator.schemaName;
import static com.dremio.plugins.elastic.ElasticBaseTestQuery.TestNameGenerator.tableName;
import static com.dremio.plugins.elastic.ElasticsearchType.DATE;
import static com.dremio.plugins.elastic.ElasticsearchType.INTEGER;
import static com.dremio.plugins.elastic.ElasticsearchType.STRING;
import com.dremio.service.namespace.SourceTableDefinition;

/* junit imports */
import static org.junit.Assert.assertTrue;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Assume;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.math.BigDecimal;


import com.dremio.exec.record.BatchSchema;

import com.google.common.collect.ImmutableMap;

/* Test if hidden indices are queryable or not */
public class TestHiddenIndices extends ElasticBaseTestQuery {

  public static ElasticsearchStoragePlugin2 plugin;

  @BeforeClass
  public static void beforeStart() {
    Assume.assumeFalse(ElasticsearchCluster.USE_EXTERNAL_ES5);
  }


  @Before
  public void before() throws Exception {
    super.before();
    plugin = (ElasticsearchStoragePlugin2)
        getSabotContext().getStorage().getPlugin(ElasticsearchStoragePluginConfig.NAME).getStoragePlugin2();
  }

  @Test
  public void testHiddenIndex() throws Exception {


      schema = ".hidden";

      ElasticsearchCluster.ColumnData[] data = new ElasticsearchCluster.ColumnData[]{
              new ElasticsearchCluster.ColumnData("location", STRING, new Object[][]{
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
      Iterable<SourceTableDefinition> tables = plugin.getDatasets("", true);

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
