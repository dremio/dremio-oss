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
package com.dremio.exec.planner.sql.handlers;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;

import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.users.UserNotFoundException;

/**
 * Validates dataset access by traversing parent datasets
 */
public class ViewAccessEvaluator implements Runnable {
  final CountDownLatch latch;
  final RelNode rel;
  final SqlHandlerConfig config;
  Exception exception;

  public ViewAccessEvaluator(RelNode rel, SqlHandlerConfig config) {
    this.rel = rel;
    this.config = config;
    latch = new CountDownLatch(1);
    exception = null;
  }

  public CountDownLatch getLatch() {
    return latch;
  }

  public Exception getException() {
    return exception;
  }

  @Override
  public void run() {
    final Thread currentThread = Thread.currentThread();
    final String originalName = currentThread.getName();
    currentThread.setName(config.getContext().getQueryId() + ":foreman-access-evaluation");
    try {
      final List<List<String>> topExpansionPaths = new ArrayList<>();
      rel.accept(new RelShuttleImpl() {
        public RelNode visit(RelNode other) {
          if (other instanceof ExpansionNode) {
            ExpansionNode expansionNode = ((ExpansionNode) other);
            topExpansionPaths.add(expansionNode.getPath().getPathComponents());
            return other;
          }
          return super.visit(other);
        }
      });
      if (!topExpansionPaths.isEmpty()) {
        final List<DremioTable> tables = new ArrayList<>();
        for (List<String> path : topExpansionPaths) {
          DremioTable table = config.getConverter().getCatalogReader().getTable(path).getTable();
          tables.add(table);
        }
        validateViewAccess(tables, config.getConverter().getCatalogReader().withCheckValidity(false), config.getContext().getQueryUserName());
      }
    } catch (Exception e) {
      exception = e;
    } finally {
      latch.countDown();
      currentThread.setName(originalName);
    }
  }

  private void validateViewAccess(List<DremioTable> tables, DremioCatalogReader catalogReader, String queryUser) {
    for (DremioTable table : tables) {
      DatasetConfig datasetConfig = table.getDatasetConfig();
      if (datasetConfig != null) {
        String owner = datasetConfig.getOwner();
        final DremioCatalogReader catalogReaderWithUser = owner == null ? catalogReader :
          catalogReader.withSchemaPathAndUser(table.getPath().getPathComponents(), owner, false);
        VirtualDataset vds = datasetConfig.getVirtualDataset();
        if (vds != null && vds.getParentsList() != null) {
          validateViewAccess(
            vds.getParentsList().stream()
              .map(parent -> {
                DremioPrepareTable dremioTable;
                try {
                  dremioTable = catalogReaderWithUser.getTable(parent.getDatasetPathList());
                } catch (RuntimeException ex) {
                  if (!(ex.getCause() instanceof UserNotFoundException)) {
                    throw ex;
                  }
                  dremioTable = catalogReader.withSchemaPathAndUser(table.getPath().getPathComponents(), queryUser, false)
                    .getTable(parent.getDatasetPathList());
                }
                if (dremioTable != null) {
                  return dremioTable.getTable();
                }
                return null;
              }).filter(Objects::nonNull).collect(Collectors.toList()),
            catalogReader,
            queryUser);
        }
      }
    }
  }
}
