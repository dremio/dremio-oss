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
package com.dremio.service.autocomplete.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.service.autocomplete.columns.Column;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class AutocompleteSchemaProvider {

  private static final String HOME_PREFIX = "@";
  private final NamespaceService namespaceService;
  private final ConnectionReader connectionReader;
  private final EntityExplorer entityExplorer;
  private final List<String> context;
  private final String currentUser;

  public AutocompleteSchemaProvider(String currentUser,
                                    NamespaceService namespaceService,
                                    ConnectionReader connectionReader,
                                    EntityExplorer entityExplorer,
                                    List<String> context) {
    this.currentUser = currentUser;
    this.namespaceService = namespaceService;
    this.connectionReader = connectionReader;
    this.entityExplorer = entityExplorer;
    this.context = context;
  }

  /**
   * Returns data source columns by the FULL path provided.
   * NOTE: it ignores current context provided by the user and assumes that the path includes the context as its part.
   */
  public ImmutableSet<Column> getColumnsByFullPath(List<String> fullPath) {
    try {
      NamespaceKey namespaceKey = new NamespaceKey(fullPath);
      DremioTable table = entityExplorer.getTableNoResolve(namespaceKey);
      if (table == null) {
        return ImmutableSet.of();
      }
      ImmutableSet.Builder<Column> columns = new ImmutableSet.Builder<>();
      for (Field field : table.getSchema()) {
        SqlTypeName calciteType = CalciteArrowHelper.getCalciteTypeFromMinorType(CompleteType.fromField(field).toMinorType());
        columns.add(Column.typedColumn(field.getName(), calciteType));
      }
      return columns.build();
    } catch (Exception ex) {
      return ImmutableSet.of();
    }
  }

  /**
   * Returns all child nodes for a node by path.
   * The path can be either relative to the current context OR part of the global scope.
   * Current context has a higher priority
   */
  public ImmutableList<Node> getChildrenInScope(List<String> path) {
    List<String> pathTokens = getPathAtCursor(path);
    if (context.isEmpty() && pathTokens.isEmpty()) {
      return getRootLevelNodes();
    }
    return getChildrenInScope(new NamespaceKey(pathLocalToContext(pathTokens)))
      .orElseGet(() ->
        // lookup in the parent context
        getChildrenInScope(new NamespaceKey(pathTokens))
          .orElse(ImmutableList.of())
      );
  }

  private ImmutableList<Node> getRootLevelNodes() {
    try {
      ImmutableList.Builder<Node> nodes = new ImmutableList.Builder<>();
      getCurrentUserHome().ifPresent(nodes::add);
      namespaceService.getSpaces().stream().map(AutocompleteSchemaProvider::convertSpaceToNode).forEach(nodes::add);
      namespaceService.getSources().stream()
        .filter(source -> !isInternalSource(source))
        .map(AutocompleteSchemaProvider::convertSourcesToNode).forEach(nodes::add);
      return nodes.build();
    } catch (Exception ex) {
      return ImmutableList.of();
    }
  }

  private Optional<ImmutableList<Node>> getChildrenInScope(NamespaceKey namespaceKey) {
    try {
      return Optional.of(
        namespaceService.list(namespaceKey).stream()
          .map(this::convertToNode)
          .collect(ImmutableList.toImmutableList())
      );
    } catch (Exception ex) {
      return Optional.empty();
    }
  }

  private List<String> pathLocalToContext(List<String> path) {
    List<String> pathLocalToContext = new ArrayList<>(context);
    pathLocalToContext.addAll(path);
    return pathLocalToContext;
  }

  private boolean isInternalSource(SourceConfig config) {
    return connectionReader.getConnectionConf(config).isInternal();
  }

  private Optional<Node> getCurrentUserHome() {
    try {
      NamespaceKey homePath = new NamespaceKey(getHomeName(currentUser));
      // This is to check that it exists
      namespaceService.getHome(homePath);
      return Optional.of(getHomeNode());
    } catch (NamespaceException e) {
      return Optional.empty();
    }
  }

  private static String getHomeName(String user) {
    return HOME_PREFIX + user;
  }

  protected static List<String> getPathAtCursor(List<String> pathTokens) {
    int cursorIndex = Iterables.indexOf(pathTokens, pathToken -> pathToken.endsWith(Cursor.CURSOR_CHARACTER));
    if (cursorIndex >= 0) {
      pathTokens = pathTokens.subList(0, cursorIndex);
    }
    return pathTokens;
  }

  private Node convertToNode(NameSpaceContainer item) {
    switch (item.getType()) {
      case FOLDER:
        return new Node(item.getFolder().getName(), Node.Type.FOLDER);
      case HOME:
        return getHomeNode();
      case SOURCE:
        return convertSourcesToNode(item.getSource());
      case SPACE:
        return convertSpaceToNode(item.getSpace());
      case DATASET:
        return new Node(item.getDataset().getName(), convertDatasetType(item.getDataset().getType()));
      default:
        throw new UnsupportedOperationException();
    }
  }

  private Node getHomeNode() {
    return new Node(getHomeName(currentUser), Node.Type.HOME);
  }

  private static Node convertSpaceToNode(SpaceConfig config) {
    return new Node(config.getName(), Node.Type.SPACE);
  }

  private static Node convertSourcesToNode(SourceConfig config) {
    return new Node(config.getName(), Node.Type.SOURCE);
  }

  private static Node.Type convertDatasetType(DatasetType datasetType) {
    switch (datasetType) {
      case PHYSICAL_DATASET:
      case PHYSICAL_DATASET_HOME_FILE:
      case PHYSICAL_DATASET_HOME_FOLDER:
      case PHYSICAL_DATASET_SOURCE_FOLDER:
      case PHYSICAL_DATASET_SOURCE_FILE:
        return Node.Type.PHYSICAL_SOURCE;
      case VIRTUAL_DATASET:
        return Node.Type.VIRTUAL_SOURCE;
      default:
        throw new UnsupportedOperationException();
    }
  }
}
