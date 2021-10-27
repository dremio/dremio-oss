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
package com.dremio.exec.planner.sql.handlers.direct;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.parser.SqlAlterDatasetReflectionRouting;
import com.dremio.resource.common.ReflectionRoutingManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * Handler for <code>ALTER DATASET .. ROUTE REFLECTIONS TO ..</code> command.
 */
public abstract class CommonReflectionRoutingHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CommonReflectionRoutingHandler.class);

  private final Catalog catalog;
  protected final ReflectionRoutingManager reflectionRoutingManager;

  public CommonReflectionRoutingHandler(QueryContext context){
    this.catalog = context.getCatalog();
    reflectionRoutingManager = context.getReflectionRoutingManager();
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlAlterDatasetReflectionRouting reflectionRouting = SqlNodeUtil.unwrap(sqlNode, SqlAlterDatasetReflectionRouting.class);
    checkRoutingSyntax(reflectionRouting);

    String destinationName = null;
    if(!reflectionRouting.isDefault()) {
      destinationName = reflectionRouting.getQueueOrEngineName().toString();
      verifyRoutingDestination(destinationName);
    }

    final SchemaUtilities.TableWithPath table = SchemaUtilities.verify(catalog, reflectionRouting.getTblName());
    DatasetConfig datasetConfig = table.getTable().getDatasetConfig();
    String successMessage = String.format("OK: Reflections dependent on %s will be refreshed using ", datasetConfig.getName());

    //set destination
    setRoutingDestination(datasetConfig, destinationName);
    //refresh catalog
    final NamespaceKey namespaceKey = new NamespaceKey(datasetConfig.getFullPathList());
    catalog.addOrUpdateDataset(namespaceKey, datasetConfig);
    return Collections.singletonList(SimpleCommandResult.successful(
      successMessage.concat(getRoutingDestination(reflectionRouting.isDefault(), destinationName))));
  }

  public void checkRoutingSyntax(SqlAlterDatasetReflectionRouting reflectionRouting) throws Exception {
    throw new UnsupportedOperationException("operation not supported");
  }

  public void verifyRoutingDestination(String destinationName) throws Exception {
    throw new UnsupportedOperationException("operation not supported");
  }

  public void setRoutingDestination(DatasetConfig config, String destinationName) throws Exception {
    throw new UnsupportedOperationException("operation not supported");
  }

  protected String getRoutingDestination(boolean isDefault, String destinationName) throws Exception {
    throw new UnsupportedOperationException("operation not supported");
  }
}
