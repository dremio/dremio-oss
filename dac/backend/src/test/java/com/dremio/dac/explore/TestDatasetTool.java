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
package com.dremio.dac.explore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.VersionedDatasetId;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.History;
import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.dac.proto.model.dataset.From;
import com.dremio.dac.proto.model.dataset.FromTable;
import com.dremio.dac.proto.model.dataset.NameDatasetRef;
import com.dremio.dac.proto.model.dataset.Transform;
import com.dremio.dac.proto.model.dataset.TransformType;
import com.dremio.dac.proto.model.dataset.TransformUpdateSQL;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.JobsVersionContext;
import com.dremio.service.namespace.dataset.DatasetVersion;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.core.SecurityContext;
import org.junit.Assert;
import org.junit.Test;

/** Tests for DatasetTool */
public class TestDatasetTool {
  @Test
  public void testBrokenHistory() throws Exception {
    DatasetPath datasetPath = new DatasetPath(Arrays.asList("space", "dataset"));
    DatasetVersion current = new DatasetVersion("123");
    DatasetVersion tip = new DatasetVersion("456");
    DatasetVersion broken = new DatasetVersion("001");

    // tip dataset whose previous version points at an non existent history
    VirtualDatasetUI tipDataset = buildDataset(datasetPath, tip, broken.getVersion());

    DatasetVersionMutator datasetVersionMutator = mock(DatasetVersionMutator.class);
    // the tip history request
    when(datasetVersionMutator.getVersion(datasetPath, tip)).thenReturn(tipDataset);
    when(datasetVersionMutator.get(any())).thenReturn(tipDataset);
    when(datasetVersionMutator.getVersion(datasetPath, broken))
        .thenThrow(DatasetVersionNotFoundException.class);

    final DatasetTool tool = buildDatasetTool(datasetVersionMutator);

    History history = tool.getHistory(datasetPath, current, tip);
    Assert.assertEquals(1, history.getItems().size());
  }

  @Test
  public void testRewriteHistory() throws Exception {
    DatasetPath datasetPath = new DatasetPath(Arrays.asList("space", "dataset"));
    DatasetPath newDatasetPath = new DatasetPath(Arrays.asList("space", "dataset-new"));
    DatasetVersion history1 = new DatasetVersion("234");
    DatasetVersion history2 = new DatasetVersion("123");
    DatasetVersion tip = new DatasetVersion("456");

    VirtualDatasetUI tipDataset = buildDataset(datasetPath, tip, history1.getVersion());
    VirtualDatasetUI history1Dataset = buildDataset(datasetPath, history1, history2.getVersion());
    VirtualDatasetUI history2Dataset = buildDataset(datasetPath, history2, null);

    VirtualDatasetUI newTipDataset = buildDataset(newDatasetPath, tip, null);
    // Set previous version to null
    newTipDataset.setPreviousVersion(tipDataset.getPreviousVersion());
    VirtualDatasetUI newHistory1Dataset =
        buildDataset(newDatasetPath, history1, history2.getVersion());
    VirtualDatasetUI newHistory2Dataset = buildDataset(newDatasetPath, history2, null);

    DatasetVersionMutator datasetVersionMutator = mock(DatasetVersionMutator.class);
    when(datasetVersionMutator.getVersion(datasetPath, tip)).thenReturn(tipDataset);
    when(datasetVersionMutator.getVersion(datasetPath, history1)).thenReturn(history1Dataset);
    when(datasetVersionMutator.getVersion(datasetPath, history2)).thenReturn(history2Dataset);
    when(datasetVersionMutator.get(datasetPath)).thenReturn(tipDataset);

    when(datasetVersionMutator.getVersion(newDatasetPath, tip)).thenReturn(newTipDataset);
    when(datasetVersionMutator.getVersion(newDatasetPath, history1)).thenReturn(newHistory1Dataset);
    when(datasetVersionMutator.getVersion(newDatasetPath, history2)).thenReturn(newHistory2Dataset);
    when(datasetVersionMutator.get(newDatasetPath)).thenReturn(newTipDataset);

    final DatasetTool tool = buildDatasetTool(datasetVersionMutator);

    tool.rewriteHistory(newTipDataset, newDatasetPath);

    History newHistory = tool.getHistory(newDatasetPath, tip, tip);
    // Make sure coped VDS has 3 history items.
    Assert.assertEquals(3, newHistory.getItems().size());
  }

  @Test
  public void testSourceVersionMapping() {
    final DatasetTool datasetTool =
        new DatasetTool(
            mock(DatasetVersionMutator.class),
            mock(JobsService.class),
            mock(QueryExecutor.class),
            mock(SecurityContext.class));
    Map<String, VersionContextReq> references = new HashMap<>();
    references.put(
        "source1", new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, "branch"));
    references.put(
        "source2", new VersionContextReq(VersionContextReq.VersionContextType.TAG, "tag"));
    references.put(
        "source3",
        new VersionContextReq(
            VersionContextReq.VersionContextType.COMMIT,
            "d0628f078890fec234b98b873f9e1f3cd140988a"));

    Map<String, JobsVersionContext> expectedSourceVersionMapping = new HashMap<>();
    expectedSourceVersionMapping.put(
        "source1", new JobsVersionContext(JobsVersionContext.VersionContextType.BRANCH, "branch"));
    expectedSourceVersionMapping.put(
        "source2", new JobsVersionContext(JobsVersionContext.VersionContextType.TAG, "tag"));
    expectedSourceVersionMapping.put(
        "source3",
        new JobsVersionContext(
            JobsVersionContext.VersionContextType.BARE_COMMIT,
            "d0628f078890fec234b98b873f9e1f3cd140988a"));

    assertThat(datasetTool.createSourceVersionMapping(references))
        .usingRecursiveComparison()
        .isEqualTo(expectedSourceVersionMapping);
  }

  @Test
  public void testUpdateVirtualDatasetId() {
    final Catalog catalog = mock(Catalog.class);
    final StoragePlugin plugin = mock(FakeVersionedPlugin.class);
    final String sourceName = "source1";
    final List<String> tableKey =
        Stream.of(sourceName, "table").collect(Collectors.toCollection(ArrayList::new));
    final DatasetPath datasetPath = DatasetTool.TMP_DATASET_PATH;
    final String contentId = "8d43f534-b97e-48e8-9b39-35e6309ed110";
    final From from = new FromTable(String.join(".", tableKey)).wrap();
    final Map<String, VersionContextReq> references =
        Collections.singletonMap(
            sourceName,
            new VersionContextReq(VersionContextReq.VersionContextType.BRANCH, "branch"));
    final Map<String, VersionContext> versionContextMapping =
        DatasetResourceUtils.createSourceVersionMapping(references);
    final VersionedDatasetId versionedDatasetId =
        VersionedDatasetId.newBuilder()
            .setTableKey(tableKey)
            .setContentId(contentId)
            .setTableVersionContext(new TableVersionContext(TableVersionType.BRANCH, "branch"))
            .build();

    when(catalog.getSource(sourceName)).thenReturn(plugin);
    when(plugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(catalog.resolveCatalog(versionContextMapping)).thenReturn(catalog);
    when(catalog.getDatasetId(any())).thenReturn(versionedDatasetId.asString());

    final VirtualDatasetUI vds =
        DatasetTool.newDatasetBeforeQueryMetadata(
            datasetPath, null, from, null, null, catalog, references);

    assertThat(vds.getId()).isEqualTo(versionedDatasetId.asString());
  }

  private DatasetTool buildDatasetTool(DatasetVersionMutator datasetVersionMutator) {
    final Catalog catalog = mock(Catalog.class);
    when(datasetVersionMutator.getCatalog()).thenReturn(catalog);
    when(catalog.getSource(any())).thenReturn(mock(StoragePlugin.class));

    JobsService jobsService = mock(JobsService.class);
    when(jobsService.searchJobs(any())).thenReturn(Collections.emptyList());
    QueryExecutor executor = mock(QueryExecutor.class);

    SecurityContext securityContext =
        new SecurityContext() {
          @Override
          public Principal getUserPrincipal() {
            return new Principal() {
              @Override
              public String getName() {
                return "user";
              }
            };
          }

          @Override
          public boolean isUserInRole(String role) {
            return false;
          }

          @Override
          public boolean isSecure() {
            return false;
          }

          @Override
          public String getAuthenticationScheme() {
            return null;
          }
        };

    return new DatasetTool(datasetVersionMutator, jobsService, executor, securityContext);
  }

  private VirtualDatasetUI buildDataset(
      DatasetPath datasetPath, DatasetVersion version, String previousVersion) {
    VirtualDatasetUI dataset = new VirtualDatasetUI();
    dataset.setCreatedAt(0L);
    dataset.setFullPathList(datasetPath.toPathList());
    dataset.setVersion(version);
    if (previousVersion == null) {
      dataset.setPreviousVersion(null);
    } else {
      dataset.setPreviousVersion(
          new NameDatasetRef()
              .setDatasetVersion(previousVersion)
              .setDatasetPath(datasetPath.toString()));
    }
    Transform transform = new Transform(TransformType.updateSQL);
    transform.setUpdateSQL(new TransformUpdateSQL("sql"));
    dataset.setLastTransform(transform);

    return dataset;
  }

  /** Fake Versioned Plugin interface for test */
  private interface FakeVersionedPlugin extends VersionedPlugin, StoragePlugin {}
}
