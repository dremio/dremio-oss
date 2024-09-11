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

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import com.dremio.dac.explore.DatasetTool;
import com.dremio.dac.explore.DatasetVersionResource;
import com.dremio.dac.explore.QueryExecutor;
import com.dremio.dac.explore.Transformer;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.TransformUpdateSQL;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.service.errors.DatasetVersionNotFoundException;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.ViewCreatorFactory;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.options.OptionManager;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.base.Throwables;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Provider;
import javax.ws.rs.core.SecurityContext;
import org.apache.arrow.memory.BufferAllocator;

/** Implementation of ViewCreatorFactory for DAC backend */
public class DACViewCreatorFactory implements ViewCreatorFactory {
  private final Provider<LegacyKVStoreProvider> kvStoreProvider;
  private final Provider<JobsService> jobsService;
  private final Provider<NamespaceService.Factory> namespaceServiceFactory;
  private final Provider<SabotContext> sabotContext;
  private final Provider<CatalogService> catalogService;
  private final BufferAllocator allocator;
  private final Provider<OptionManager> optionManagerProvider;

  public DACViewCreatorFactory(
      Provider<LegacyKVStoreProvider> kvStoreProvider,
      Provider<JobsService> jobsService,
      Provider<NamespaceService.Factory> namespaceServiceFactory,
      Provider<CatalogService> catalogService,
      Provider<SabotContext> sabotContext,
      Provider<BufferAllocator> allocator,
      Provider<OptionManager> optionManagerProvider) {
    this.kvStoreProvider = kvStoreProvider;
    this.jobsService = jobsService;
    this.namespaceServiceFactory = namespaceServiceFactory;
    this.catalogService = catalogService;
    this.sabotContext = sabotContext;
    this.allocator = allocator.get().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
    this.optionManagerProvider = optionManagerProvider;
  }

  @Override
  public ViewCreator get(String userName) {
    NamespaceService namespaceService = namespaceServiceFactory.get().get(userName);
    DatasetVersionMutator datasetVersionMutator =
        new DatasetVersionMutator(
            kvStoreProvider.get(),
            jobsService.get(),
            catalogService.get(),
            optionManagerProvider.get(),
            sabotContext.get());
    return new DACViewCreator(
        userName, sabotContext.get(), datasetVersionMutator, namespaceService);
  }

  protected class DACViewCreator implements ViewCreator {
    private final String userName;
    private final JobsService jobsService = DACViewCreatorFactory.this.jobsService.get();
    private final DatasetVersionMutator datasetVersionMutator;
    private final NamespaceService namespaceService;
    private final SabotContext sabotContext;

    DACViewCreator(
        String userName,
        SabotContext sabotContext,
        DatasetVersionMutator datasetVersionMutator,
        NamespaceService namespaceService) {
      this.userName = userName;
      this.namespaceService = namespaceService;
      this.sabotContext = sabotContext;
      this.datasetVersionMutator = datasetVersionMutator;
    }

    @Override
    public void createView(
        List<String> path,
        String sql,
        List<String> sqlContext,
        boolean isVersionedSource,
        NamespaceAttribute... attributes) {
      SecurityContext securityContext = getSecurityContext();
      QueryExecutor executor = new QueryExecutor(jobsService, null, securityContext);
      DatasetTool tool = newDatasetTool(securityContext, executor);

      try {
        DatasetVersion version = DatasetVersion.newVersion();
        DatasetPath datasetPath = new DatasetPath(getPathInOriginalCase(path));
        InitialPreviewResponse response =
            tool.newUntitled(allocator, new FromSQL(sql), version, sqlContext, null, true, 0, true);
        DatasetPath tmpPath = new DatasetPath(response.getDataset().getFullPath());
        VirtualDatasetUI vds =
            datasetVersionMutator.getVersion(
                tmpPath, response.getDataset().getDatasetVersion(), isVersionedSource);

        // TODO - Remove dependency on a Resource file.
        newDatasetVersionResource(securityContext, tool, version, tmpPath, allocator)
            .save(vds, datasetPath, null, null, isVersionedSource, attributes);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void updateView(
        List<String> path, String sql, List<String> sqlContext, NamespaceAttribute... attributes) {
      SecurityContext securityContext = getSecurityContext();
      QueryExecutor executor = new QueryExecutor(jobsService, null, securityContext);

      List<String> correctedPath = getPathInOriginalCase(path);
      NamespaceKey namespaceKey = new NamespaceKey(correctedPath);

      try {
        DatasetConfig dataset = namespaceService.getDataset(namespaceKey);

        DatasetPath datasetPath = new DatasetPath(correctedPath);
        final VirtualDatasetUI virtualDataset = datasetVersionMutator.get(datasetPath);

        Transformer transformer =
            new Transformer(
                sabotContext,
                jobsService,
                namespaceService,
                datasetVersionMutator,
                executor,
                securityContext,
                catalogService.get());
        TransformUpdateSQL transformUpdateSQL = new TransformUpdateSQL();
        transformUpdateSQL.setSql(sql);
        transformUpdateSQL.setSqlContextList(sqlContext);

        VirtualDatasetUI virtualDatasetUI =
            transformer.transformWithExtract(datasetPath, virtualDataset, transformUpdateSQL);

        // copy over VirtualDatasetUI values to the pre-existing DatasetConfig
        dataset.setVirtualDataset(DatasetsUtil.toVirtualDataset(virtualDatasetUI));
        dataset.setRecordSchema(virtualDatasetUI.getRecordSchema());

        namespaceService.addOrUpdateDataset(namespaceKey, dataset, attributes);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    private SecurityContext getSecurityContext() {
      return new SecurityContext() {
        @Override
        public Principal getUserPrincipal() {
          return new Principal() {
            @Override
            public String getName() {
              return userName;
            }
          };
        }

        @Override
        public boolean isUserInRole(String role) {
          return true;
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
    }

    protected DatasetTool newDatasetTool(SecurityContext securityContext, QueryExecutor executor) {
      return new DatasetTool(datasetVersionMutator, jobsService, executor, securityContext);
    }

    protected DatasetVersionResource newDatasetVersionResource(
        SecurityContext securityContext,
        DatasetTool tool,
        DatasetVersion version,
        DatasetPath tmpPath,
        BufferAllocator allocator) {
      return new DatasetVersionResource(
          null,
          datasetVersionMutator,
          null,
          null,
          null,
          tool,
          null,
          securityContext,
          tmpPath,
          version,
          allocator);
    }

    @Override
    public void dropView(List<String> path) {
      DatasetPath datasetPath = new DatasetPath(path);
      try {
        try {
          final VirtualDatasetUI virtualDataset = datasetVersionMutator.get(datasetPath);
          String savedTag = virtualDataset.getSavedTag();
          datasetVersionMutator.deleteDataset(datasetPath, savedTag);
        } catch (DatasetVersionNotFoundException e) {
          datasetVersionMutator.deleteDataset(datasetPath, null);
        }
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
  }

  public SabotContext getSabotContext() {
    return sabotContext.get();
  }

  public CatalogService getCatalogService() {
    return catalogService.get();
  }

  @Override
  public void close() {
    allocator.close();
  }

  @Override
  public void start() {}

  /**
   * Convert the path to the original case of entity already exists in the namespace
   *
   * @param path
   * @return path in original case
   */
  private List<String> getPathInOriginalCase(List<String> path) {
    Catalog catalog =
        catalogService
            .get()
            .getCatalog(
                MetadataRequestOptions.of(
                    SchemaConfig.newBuilder(CatalogUser.from(SYSTEM_USERNAME)).build()));

    List<NamespaceKey> keys = new ArrayList<>();
    for (int i = path.size(); i > 0; i--) {
      keys.add(new NamespaceKey(path.subList(0, i)));
    }
    List<NameSpaceContainer> containers = catalog.getEntities(keys);
    for (int i = 0; i < containers.size() - 1; i++) {
      if (containers.get(i) != null) {
        List<String> pathInOriginalCase = containers.get(i).getFullPathList();
        pathInOriginalCase.addAll(path.subList(pathInOriginalCase.size(), path.size()));
        return pathInOriginalCase;
      }
    }

    return path;
  }
}
