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

import java.security.Principal;
import java.util.List;

import javax.inject.Provider;
import javax.ws.rs.core.SecurityContext;

import com.dremio.dac.explore.DatasetTool;
import com.dremio.dac.explore.DatasetVersionResource;
import com.dremio.dac.explore.QueryExecutor;
import com.dremio.dac.explore.Transformer;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.TransformUpdateSQL;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.dac.util.DatasetsUtil;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.catalog.ViewCreatorFactory;
import com.dremio.exec.server.ContextService;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.InitializerRegistry;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.ReflectionService;
import com.google.common.base.Throwables;

/**
 * Implementation of ViewCreatorFactory for DAC backend
 */
public class DACViewCreatorFactory implements ViewCreatorFactory {
  private final Provider<InitializerRegistry> initializerRegistry;
  private final Provider<KVStoreProvider> kvStoreProvider;
  private final Provider<JobsService> jobsService;
  private final Provider<NamespaceService.Factory> namespaceServiceFactory;
  private final Provider<ReflectionService> reflectionService;
  private final Provider<ContextService> contextService;
  private final Provider<CatalogService> catalogService;

  public DACViewCreatorFactory(Provider<InitializerRegistry> initializerRegistry,
                                Provider<KVStoreProvider> kvStoreProvider,
                                Provider<JobsService> jobsService,
                                Provider<NamespaceService.Factory> namespaceServiceFactory,
                                Provider<ReflectionService> reflectionService,
                                Provider<CatalogService> catalogService,
                                Provider<ContextService> contextService
  ) {
    this.initializerRegistry = initializerRegistry;
    this.kvStoreProvider = kvStoreProvider;
    this.jobsService = jobsService;
    this.namespaceServiceFactory = namespaceServiceFactory;
    this.catalogService = catalogService;
    this.reflectionService = reflectionService;
    this.contextService = contextService;
  }

  @Override
  public ViewCreator get(String userName) {
    return new DACViewCreator(userName);
  }

  /**
   *
   */
  protected class DACViewCreator implements ViewCreator {
    private final String userName;
    private final JobsService jobsService = DACViewCreatorFactory.this.jobsService.get();
    private final DatasetVersionMutator datasetService;
    private final NamespaceService namespaceService;

    DACViewCreator(String userName) {
      this.userName = userName;
      namespaceService = namespaceServiceFactory.get().get(userName);
      datasetService = new DatasetVersionMutator(initializerRegistry.get(), kvStoreProvider.get(), namespaceService,
        jobsService, catalogService.get());
    }

    @Override
    public void createView(List<String> path, String sql, List<String> sqlContext, NamespaceAttribute... attributes) {
      SecurityContext securityContext = getSecurityContext();
      QueryExecutor executor = new QueryExecutor(jobsService, null, securityContext);
      DatasetTool tool = newDatasetTool(securityContext, executor);

      try {
        DatasetVersion version = DatasetVersion.newVersion();
        DatasetPath datasetPath = new DatasetPath(path);
        InitialPreviewResponse response = tool.newUntitled(new FromSQL(sql), version, sqlContext, null, true, 0);
        DatasetPath tmpPath = new DatasetPath(response.getDataset().getFullPath());
        VirtualDatasetUI vds = datasetService.getVersion(tmpPath, response.getDataset().getDatasetVersion());
        newDatasetVersionResource(securityContext, tool, version, tmpPath).save(vds, datasetPath, null, attributes);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void updateView(List<String> path, String sql, List<String> sqlContext, NamespaceAttribute... attributes) {
      SecurityContext securityContext = getSecurityContext();
      QueryExecutor executor = new QueryExecutor(jobsService, null, securityContext);
      DatasetVersion version = DatasetVersion.newVersion();

      NamespaceKey namespaceKey = new NamespaceKey(path);

      try {
        DatasetConfig dataset = namespaceService.getDataset(namespaceKey);

        DatasetPath datasetPath = new DatasetPath(path);
        final VirtualDatasetUI virtualDataset = datasetService.get(datasetPath);

        Transformer transformer = new Transformer(contextService.get().get(), namespaceService, datasetService, executor, securityContext);
        TransformUpdateSQL transformUpdateSQL = new TransformUpdateSQL();
        transformUpdateSQL.setSql(sql);
        transformUpdateSQL.setSqlContextList(sqlContext);

        VirtualDatasetUI virtualDatasetUI = transformer.transformWithExtract(version, datasetPath, virtualDataset, transformUpdateSQL);

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
      return new DatasetTool(datasetService, jobsService, executor, securityContext);
    }

    protected DatasetVersionResource newDatasetVersionResource(SecurityContext securityContext, DatasetTool tool,
        DatasetVersion version, DatasetPath tmpPath) {
      return new DatasetVersionResource(null, datasetService, null, null, null, tool, null, securityContext, tmpPath, version);
    }

    @Override
    public void dropView(List<String> path) {
      try {
        DatasetPath datasetPath = new DatasetPath(path);
        final VirtualDatasetUI virtualDataset = datasetService.get(datasetPath);
        String savedTag = virtualDataset.getSavedTag();
        datasetService.deleteDataset(datasetPath, savedTag);
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
  }

  @Override
  public void close() {}

  @Override
  public void start() {}
}
