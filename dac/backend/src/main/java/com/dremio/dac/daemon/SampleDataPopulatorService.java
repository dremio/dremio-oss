/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.daemon;

import javax.inject.Provider;
import javax.ws.rs.core.SecurityContext;

import com.dremio.common.AutoCloseables;
import com.dremio.dac.model.usergroup.UserName;
import com.dremio.dac.server.DACSecurityContext;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.source.SourceService;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.InitializerRegistry;
import com.dremio.service.Service;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.users.SystemUser;
import com.dremio.service.users.UserService;

/**
 * Starts the SampleDataPopulator
 */
public class SampleDataPopulatorService implements Service {
  private final Provider<SabotContext> contextProvider;
  private final Provider<UserService> userService;
  private final Provider<KVStoreProvider> kvStore;
  private final Provider<InitializerRegistry> init;
  private final Provider<JobsService> jobsService;
  private final Provider<CatalogService> catalogService;
  private final Provider<ReflectionServiceHelper> reflectionHelper;
  private final Provider<ConnectionReader> connectionReader;

  private SampleDataPopulator sample;

  private final boolean prepopulate;
  private final boolean addDefaultUser;

  public SampleDataPopulatorService(
    Provider<SabotContext> contextProvider,
    Provider<KVStoreProvider> kvStore,
    Provider<UserService> userService,
    Provider<InitializerRegistry> init,
    Provider<JobsService> jobsService,
    Provider<CatalogService> catalogService,
    Provider<ReflectionServiceHelper> reflectionHelper,
    Provider<ConnectionReader> connectionReader, boolean prepopulate,
    boolean addDefaultUser) {
    this.contextProvider = contextProvider;
    this.kvStore = kvStore;
    this.userService = userService;
    this.init = init;
    this.jobsService = jobsService;
    this.catalogService = catalogService;
    this.reflectionHelper = reflectionHelper;
    this.connectionReader = connectionReader;
    this.prepopulate = prepopulate;
    this.addDefaultUser = addDefaultUser;
  }

  @Override
  public void start() throws Exception {
    final KVStoreProvider kv = kvStore.get();
    final NamespaceService ns = contextProvider.get().getNamespaceService(SystemUser.SYSTEM_USERNAME);

    if (addDefaultUser) {
      SampleDataPopulator.addDefaultFirstUser(userService.get(), ns);
    }

    if (prepopulate) {
      final DatasetVersionMutator data = new DatasetVersionMutator(init.get(), kv, ns, jobsService.get(),
        catalogService.get());
      SecurityContext context = new DACSecurityContext(new UserName(SystemUser.SYSTEM_USERNAME), SystemUser.SYSTEM_USER, null);
      final SourceService ss = new SourceService(ns, data, catalogService.get(), reflectionHelper.get(), connectionReader.get(), context);
      sample = new SampleDataPopulator(
          contextProvider.get(),
          ss,
          data,
          userService.get(),
          contextProvider.get().getNamespaceService(SampleDataPopulator.DEFAULT_USER_NAME),
          SampleDataPopulator.DEFAULT_USER_NAME
      );

      sample.populateInitialData();
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(sample);
  }
}
