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

import static com.dremio.exec.planner.sql.parser.SqlAlterDatasetReflectionRouting.RoutingType;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.parser.SqlAlterDatasetReflectionRouting;
import com.dremio.exec.planner.sql.parser.SqlTableVersionSpec;
import com.dremio.resource.common.ReflectionRoutingManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;

/**
 * Handler for <code>ALTER DATASET .. ROUTE REFLECTIONS TO ..</code> command.
 */
public abstract class CommonReflectionRoutingHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CommonReflectionRoutingHandler.class);

  private final Catalog catalog;
  protected final ReflectionRoutingManager reflectionRoutingManager;
  private final NamespaceService namespaceService;
  private final QueryContext context;

  public CommonReflectionRoutingHandler(QueryContext context){
    this.catalog = context.getCatalog();
    this.context = context;
    reflectionRoutingManager = context.getReflectionRoutingManager();
    this.namespaceService = context.getNamespaceService(SYSTEM_USERNAME);
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

    String successMessage;

    boolean routingInheritanceEnabled = context.getOptions().getOption(PlannerSettings.REFLECTION_ROUTING_INHERITANCE_ENABLED);

    switch(RoutingType.valueOf(reflectionRouting.getType().toString())) {
      case TABLE:
        successMessage = setRoutingForTable(reflectionRouting, destinationName);
        break;
      case FOLDER:
        if (!routingInheritanceEnabled) {
          throw new UnsupportedOperationException("This feature is unavailable at this time.");
        }
        successMessage = setRoutingForFolder(reflectionRouting, destinationName);
        break;
      case SPACE:
        if (!routingInheritanceEnabled) {
          throw new UnsupportedOperationException("This feature is unavailable at this time.");
        }
        successMessage = setRoutingForSpace(reflectionRouting, destinationName);
        break;
      default:
        throw new UnsupportedOperationException(
          String.format("cannot set reflection routing for unknown type %s", reflectionRouting.getType()));
    }

    return Collections.singletonList(SimpleCommandResult.successful(
      successMessage.concat(getRoutingDestination(reflectionRouting.isDefault(), destinationName))));
  }

  public String setRoutingForTable(SqlAlterDatasetReflectionRouting reflectionRouting, String destinationName) throws Exception {
    final SchemaUtilities.TableWithPath table = SchemaUtilities.verify(catalog, reflectionRouting.getName(), context.getSession(), SqlTableVersionSpec.NOT_SPECIFIED, context.getOptions());
    DatasetConfig datasetConfig = table.getTable().getDatasetConfig();

    //set destination
    setRoutingDestination(datasetConfig, destinationName);
    //refresh catalog
    final NamespaceKey namespaceKey = new NamespaceKey(datasetConfig.getFullPathList());
    catalog.addOrUpdateDataset(namespaceKey, datasetConfig);

    return String.format("OK: Reflections dependent on %s will be refreshed using ",
      datasetConfig.getFullPathList());
  }

  public String setRoutingForFolder(SqlAlterDatasetReflectionRouting reflectionRouting, String destinationName) throws Exception {
    List<String> folderName = reflectionRouting.getName().names;

    NamespaceKey folderKey = catalog.resolveSingle(new NamespaceKey(folderName));

    FolderConfig folderConfig;

    //If folder config is in namespace, grab it. Otherwise, create it and add to namespace.
    try {
      folderConfig = namespaceService.getFolder(folderKey);
    } catch (NamespaceNotFoundException ex) {
      folderConfig = new FolderConfig()
        .setName(folderKey.getLeaf())
        .setFullPathList(folderKey.getPathComponents());
    }

    //set destination
    setRoutingDestination(folderConfig, destinationName);
    //refresh catalog
    namespaceService.addOrUpdateFolder(folderKey, folderConfig);

    return String.format("OK: Reflections inside folder %s will be refreshed using ",
      folderConfig.getFullPathList());
  }

  public String setRoutingForSpace(SqlAlterDatasetReflectionRouting reflectionRouting, String destinationName) throws Exception {
    NamespaceKey spaceKey = new NamespaceKey(reflectionRouting.getName().names);

    SpaceConfig spaceConfig;

    try {
      spaceConfig = namespaceService.getSpace(spaceKey);
    } catch (NamespaceNotFoundException ex) {
      throw new UnsupportedOperationException(
        String.format("cannot find space %s", spaceKey.getName()));
    }

    //set destination
    setRoutingDestination(spaceConfig, destinationName);
    //refresh catalog
    namespaceService.addOrUpdateSpace(new NamespaceKey(spaceConfig.getName()), spaceConfig);

    return String.format("OK: Reflections inside space %s will be refreshed using ",
      spaceConfig.getName());
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

  public void setRoutingDestination(FolderConfig config, String destinationName) throws Exception {
    throw new UnsupportedOperationException("operation not supported");
  }

  public void setRoutingDestination(SpaceConfig config, String destinationName) throws Exception {
    throw new UnsupportedOperationException("operation not supported");
  }

  protected String getRoutingDestination(boolean isDefault, String destinationName) {
    throw new UnsupportedOperationException("operation not supported");
  }
}
