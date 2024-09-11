/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import static org.junit.Assert.assertTrue;

import com.dremio.common.util.TestTools;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/* Test if hidden indices are queryable or not */
public class ITTestHiddenIndices extends ElasticBaseTestQuery {

  public static ElasticsearchStoragePlugin plugin;

  @BeforeClass
  public static void beforeStart() {
    Assume.assumeFalse(ElasticsearchCluster.USE_EXTERNAL_ES5);
  }

  @Override
  @Before
  public void before() throws Exception {
    super.before();
    plugin = getCatalogService().getSource("elasticsearch");
  }

  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  @Test
  public void testHiddenIndex() throws Exception {

    schema = ".hidden";

    ElasticsearchCluster.ColumnData[] data =
        new ElasticsearchCluster.ColumnData[] {
          new ElasticsearchCluster.ColumnData(
              "location", TEXT, new Object[][] {{"San Francisco"}, {"Oakland"}, {"San Jose"}})
        };

    // load up a hidden schema
    load(schema, table, data);

    schema = "nothidden";

    // load up a not hidden schema
    load(schema, table, data);

    // get all the datasets in the plugin
    Iterator<? extends DatasetHandle> handles = plugin.listDatasetHandles().iterator();

    // make sure you can find the not hidden schema
    boolean foundNotHidden = false;

    // make sure you never find the hidden one

    while (handles.hasNext()) {
      DatasetHandle table = handles.next();
      String response = MetadataObjectsUtils.toNamespaceKey(table.getDatasetPath()).getSchemaPath();
      assertTrue(!(response.contains(".hidden")));
      if (response.contains("nothidden")) {
        foundNotHidden = true;
      }
    }

    assertTrue(foundNotHidden);
  }
}
