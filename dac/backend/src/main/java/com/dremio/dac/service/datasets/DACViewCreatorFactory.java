/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

import com.dremio.common.store.ViewCreatorFactory;
import com.dremio.dac.explore.DatasetTool;
import com.dremio.dac.explore.DatasetVersionResource;
import com.dremio.dac.explore.QueryExecutor;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.proto.model.dataset.FromSQL;
import com.dremio.dac.proto.model.dataset.VirtualDatasetUI;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.service.InitializerRegistry;
import com.dremio.service.accelerator.AccelerationService;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.base.Throwables;

/**
 * Implementation of ViewCreatorFactory for DAC backend
 */
public class DACViewCreatorFactory implements ViewCreatorFactory {
  private final Provider<InitializerRegistry> initializerRegistry;
  private final Provider<KVStoreProvider> kvStoreProvider;
  private final Provider<JobsService> jobsService;
  private final Provider<NamespaceService.Factory> namespaceServiceFactory;
  private final Provider<AccelerationService> accelerationService;

  public DACViewCreatorFactory(Provider<InitializerRegistry> initializerRegistry,
                                Provider<KVStoreProvider> kvStoreProvider,
                                Provider<JobsService> jobsService,
                                Provider<NamespaceService.Factory> namespaceServiceFactory,
                                Provider<AccelerationService> accelerationService
  ) {
    this.initializerRegistry = initializerRegistry;
    this.kvStoreProvider = kvStoreProvider;
    this.jobsService = jobsService;
    this.namespaceServiceFactory = namespaceServiceFactory;

    this.accelerationService = accelerationService;
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
    private final AccelerationService accelerationService = DACViewCreatorFactory.this.accelerationService.get();
    private final DatasetVersionMutator datasetService;
    private final NamespaceService namespaceService;

    protected DACViewCreator(String userName) {
      this.userName = userName;
      namespaceService = namespaceServiceFactory.get().get(userName);
      datasetService = new DatasetVersionMutator(initializerRegistry.get(), kvStoreProvider.get(), namespaceService, jobsService);
    }

    @Override
    public void createView(List<String> path, String sql, List<String> sqlContext) {

      SecurityContext securityContext = new SecurityContext() {
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

      QueryExecutor executor = new QueryExecutor(jobsService, null, securityContext);

      DatasetTool tool = newDatasetTool(securityContext, executor);

      try {
        DatasetVersion version = new DatasetVersion(System.currentTimeMillis());
        DatasetPath datasetPath = new DatasetPath(path);
        InitialPreviewResponse response = tool.newUntitled(new FromSQL(sql), version, sqlContext, true);
        DatasetPath tmpPath = new DatasetPath(response.getDataset().getFullPath());
        VirtualDatasetUI vds = datasetService.getVersion(tmpPath, response.getDataset().getDatasetVersion());
        newDatasetVersionResource(securityContext, tool, version, tmpPath).save(vds, datasetPath, null);
      } catch (Exception e) {
        Throwables.propagate(e);
      }
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
        Long savedVersion = virtualDataset.getSavedVersion();
        datasetService.deleteDataset(datasetPath, savedVersion);

        // will only get here if user had permission to delete dataset

        // remove acceleration
        final AccelerationId id = new AccelerationId(virtualDataset.getId());
        accelerationService.remove(id);

      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
  }

  @Override
  public void close() {}

  @Override
  public void start() throws Exception {}
}
