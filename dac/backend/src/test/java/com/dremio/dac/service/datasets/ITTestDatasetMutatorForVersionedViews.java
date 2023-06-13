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
package com.dremio.dac.service.datasets;

import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectCountQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectStarQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithSource;
import static java.util.Collections.emptyList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.tools.compatibility.internal.OlderNessieServersExtension;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.server.UserExceptionMapper;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.catalog.dataplane.ITBaseTestVersioned;
import com.dremio.service.jobs.JobsService;

@ExtendWith(OlderNessieServersExtension.class)
public class ITTestDatasetMutatorForVersionedViews extends ITBaseTestVersioned {
  private BufferAllocator allocator;
  @BeforeEach
  public void setUp() throws Exception {
    allocator =
      getSabotContext().getAllocator().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
  }

  @AfterEach
  public void cleanUp() throws Exception {
    allocator.close();
  }

  @Test
  public void testSaveAsOnNewView() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final String newViewName = generateUniqueViewName();
    final List<String> newViewPath = tablePathWithFolders(newViewName);

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(tablePath));
    runQuery(insertTableQuery(tablePath));
    runQueryCheckResults(l(JobsService.class), DATAPLANE_PLUGIN_NAME, tablePath, 3, 3, allocator, null);
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    createFolders(newViewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    DatasetPath targetViewPath = new DatasetPath(tablePathWithSource(DATAPLANE_PLUGIN_NAME, newViewPath));
    saveAsVersionedDataset(targetViewPath, selectStarQuery(tablePath), emptyList(), DATAPLANE_PLUGIN_NAME, DEFAULT_BRANCH_NAME);
  }

  @Test
  public void testSaveAsOnExistingView() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(tablePath));
    runQuery(insertTableQuery(tablePath));
    runQueryCheckResults(l(JobsService.class), DATAPLANE_PLUGIN_NAME, tablePath, 3, 3, allocator, null);
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);

    DatasetPath targetViewPath = new DatasetPath(tablePathWithSource(DATAPLANE_PLUGIN_NAME, viewPath));
    final DatasetUI createdDatasetUI = createVersionedDatasetFromSQLAndSave(targetViewPath, selectStarQuery(tablePath), emptyList(), DATAPLANE_PLUGIN_NAME, DEFAULT_BRANCH_NAME);
    UserExceptionMapper.ErrorMessageWithContext errorMessage = saveAsVersionedDatasetExpectError(targetViewPath, selectStarQuery(tablePath), emptyList(), DATAPLANE_PLUGIN_NAME, DEFAULT_BRANCH_NAME);
    assertContains("The specified location already contains a view", errorMessage.getErrorMessage());
  }

  @Test
  public void testSaveAsWithConcurrentUpdates() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    final String viewName = generateUniqueViewName();
    final List<String> viewPath = tablePathWithFolders(viewName);
    final String newViewName = generateUniqueViewName();
    final List<String> newViewPath = tablePathWithFolders(newViewName);

    createFolders(tablePath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(tablePath));
    runQuery(insertTableQuery(tablePath));
    runQueryCheckResults(l(JobsService.class), DATAPLANE_PLUGIN_NAME, tablePath, 3, 3, allocator, null);
    createFolders(viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    createFolders(newViewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    DatasetPath targetViewPath = new DatasetPath(tablePathWithSource(DATAPLANE_PLUGIN_NAME, newViewPath));
    final Map<String, VersionContextReq> references = new HashMap<>();
    references.put(DATAPLANE_PLUGIN_NAME, new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, DEFAULT_BRANCH_NAME));
    InitialPreviewResponse datasetCreateResponse1 = createVersionedDatasetFromSQL(selectStarQuery(tablePath), emptyList(), DATAPLANE_PLUGIN_NAME, DEFAULT_BRANCH_NAME);
    InitialPreviewResponse datasetCreateResponse2 = createVersionedDatasetFromSQL(selectCountQuery(tablePath, "c1"), emptyList(), DATAPLANE_PLUGIN_NAME, DEFAULT_BRANCH_NAME);

    //Assert
    //First save should work
    saveAsInBranch(datasetCreateResponse1.getDataset(), targetViewPath, null, DEFAULT_BRANCH_NAME);
    //Conflicting save with null tag should fail
    saveAsInBranchExpectError(datasetCreateResponse2.getDataset(), targetViewPath, null, DEFAULT_BRANCH_NAME);
  }

  protected UserExceptionMapper.ErrorMessageWithContext saveAsVersionedDatasetExpectError(
    DatasetPath datasetPath, String sql, List<String> context, String pluginName, String branchName) {
    final Map<String, VersionContextReq> references = new HashMap<>();
    references.put(pluginName, new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, branchName));
    InitialPreviewResponse datasetCreateResponse = createVersionedDatasetFromSQL(sql, context, pluginName, branchName);
    return saveAsInBranchExpectError(datasetCreateResponse.getDataset(), datasetPath, null, branchName);

  }

  protected void saveAsVersionedDataset(
    DatasetPath datasetPath, String sql, List<String> context, String pluginName, String branchName) {
    final Map<String, VersionContextReq> references = new HashMap<>();
    references.put(pluginName, new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, branchName));
    InitialPreviewResponse datasetCreateResponse = createVersionedDatasetFromSQL(sql, context, pluginName, branchName);
    saveAsInBranch(datasetCreateResponse.getDataset(), datasetPath, null, branchName);
  }
}
