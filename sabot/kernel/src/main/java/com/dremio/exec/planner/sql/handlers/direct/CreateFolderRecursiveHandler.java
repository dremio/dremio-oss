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

import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.List;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.sql.parser.SqlCreateFolderRecursive;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;

import static com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult.successful;

import org.apache.calcite.sql.SqlNode;


/**
 * Handler for <code>CREATE FOLDER RECURSIVE</code> command.
 */
public class CreateFolderRecursiveHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateFolderRecursiveHandler.class);

  private final Catalog catalog;
  private final NamespaceService namespaceService;

  public CreateFolderRecursiveHandler(Catalog catalog, NamespaceService namespaceService) {
    this.catalog = catalog;
    this.namespaceService = namespaceService;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlCreateFolderRecursive sqlRefreshSourceStatus = SqlNodeUtil.unwrap(sqlNode, SqlCreateFolderRecursive.class);
    final NamespaceKey path = sqlRefreshSourceStatus.getPath();

    String root = path.getRoot();
    if(root.startsWith("@") || root.equalsIgnoreCase("sys") || root.equalsIgnoreCase("INFORMATION_SCHEMA")) {
      throw UserException.parseError().message("Unable to create spaces or folders in Home, sys, or INFORMATION_SCHEMA.", path).build(logger);
    }

    String message;
    try {
      // Check if space exists
      final NamespaceKey rootKey = new NamespaceKey(path.getRoot());
      try {
        namespaceService.getSpace(rootKey);
      } catch (NamespaceException nse) {
        // Space does not exist, create it
        SpaceConfig spaceConfig = new SpaceConfig()
          .setName(path.getRoot());
        namespaceService.addOrUpdateSpace(rootKey, spaceConfig);
      }

      // For each sub folder, create folder
      final List<String> folderPath = new ArrayList<>();
      folderPath.add(path.getRoot());
      for(int p=1;p<path.getPathComponents().size();p++){
        final String folderName = path.getPathComponents().get(p);
        folderPath.add(folderName);
        final NamespaceKey folderKey = new NamespaceKey(folderPath);
        try {
          namespaceService.getFolder(folderKey);
        } catch (NamespaceException nse) {
          // Space does not exist, create it
          final FolderConfig folderConfig = new FolderConfig();
          folderConfig.setFullPathList(folderPath);
          folderConfig.setName(folderName);
          namespaceService.addOrUpdateFolder(folderKey, folderConfig);
        }
      }

      message = "Created Successfully";
    } catch (Exception ex) {
      message = ex.getMessage();
    }

    return singletonList(successful(String.format(message, path.toString())));
  }
}
