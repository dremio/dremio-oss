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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.util.DremioEdition;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlAlterDatasetReflectionRouting;
import com.dremio.resource.common.RoutingQueueManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * Handler for <code>ALTER DATASET .. ROUTE REFLECTIONS TO ..</code> command.
 */
public class AlterReflectionRoutingHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AlterReflectionRoutingHandler.class);

  public static String invalidQueueMessage = "Invalid queue name";

  private final Catalog catalog;
  private final SqlHandlerConfig config;
  private final RoutingQueueManager routingQueueManager;

  public AlterReflectionRoutingHandler(Catalog catalog, SqlHandlerConfig config){
    this.catalog = catalog;
    this.config = config;
    routingQueueManager = config.getContext().getRoutingQueueManager();
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    if (DremioEdition.get() != DremioEdition.ENTERPRISE || routingQueueManager == null) {
      throw new UnsupportedOperationException("This command is not supported in this edition of Dremio.");
    }
    final SqlAlterDatasetReflectionRouting reflectionRouting = SqlNodeUtil.unwrap(sqlNode, SqlAlterDatasetReflectionRouting.class);
    final SchemaUtilities.TableWithPath table = SchemaUtilities.verify(catalog, reflectionRouting.getTblName());
    SqlIdentifier queueName = reflectionRouting.getQueueName();
    DatasetConfig datasetConfig = table.getTable().getDatasetConfig();
    String successMessage = String.format("OK: Reflections dependent on %s will be refreshed using ", datasetConfig.getName());
    if (reflectionRouting.isDefault()) {
      datasetConfig.setQueueId(null);
      successMessage = successMessage.concat("the default queue");
    } else {
      verifyQueueName(queueName);
      datasetConfig.setQueueId(routingQueueManager.getQueueIdByName(queueName.toString()));
      successMessage = successMessage.concat(String.format("queue \"%s\"", queueName));
    }
    final NamespaceKey namespaceKey = new NamespaceKey(datasetConfig.getFullPathList());
    catalog.addOrUpdateDataset(namespaceKey, datasetConfig);
    return Collections.singletonList(SimpleCommandResult.successful(successMessage));
  }

  private void verifyQueueName(SqlIdentifier queueName) throws Exception {
    if (!routingQueueManager.checkQueueExists(queueName.toString())) {
      throw new IllegalArgumentException(invalidQueueMessage);
    }
  }

}
