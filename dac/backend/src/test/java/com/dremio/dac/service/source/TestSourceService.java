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
package com.dremio.dac.service.source;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

import java.util.List;

import javax.inject.Provider;
import javax.ws.rs.core.SecurityContext;

import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.homefiles.HomeFileConf;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.InternalFileConf;
import com.dremio.exec.store.dfs.PDFSConf;
import com.dremio.exec.store.sys.SystemPluginConf;
import com.dremio.service.namespace.NamespaceService;
import com.google.common.collect.ImmutableList;

public class TestSourceService {

  private static class NonInternalConf extends ConnectionConf<NonInternalConf, StoragePlugin> {

    @Override
    public StoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
      return null;
    }

    @Override
    // Don't need to override, but making it explicit.
    public boolean isInternal() {
      return false;
    }
  }

  private SourceService getSourceService() {
    return new SourceService(
      mock(SabotContext.class),
      mock(NamespaceService.class),
      mock(DatasetVersionMutator.class),
      mock(CatalogService.class),
      mock(ReflectionServiceHelper.class),
      mock(CollaborationHelper.class),
      mock(ConnectionReader.class),
      mock(SecurityContext.class));
  }

  private static final List<ConnectionConf<?, ?>> validConnectionConfs = ImmutableList.of(new NonInternalConf());
  private static final List<ConnectionConf<?, ?>> invalidConnectionConfs = ImmutableList.of(new SystemPluginConf(), new HomeFileConf(), new PDFSConf(), new InternalFileConf());

  private void testConnectionConfs(List<ConnectionConf<?, ?>> validConnectionConfs, boolean isValid) {
    SourceService sourceService = getSourceService();
    for (ConnectionConf<?, ?> connectionConf : validConnectionConfs) {
      try {
        sourceService.validateConnectionConf(connectionConf);
      } catch (UserException e) {
        assertFalse(isValid);
      }
    }
  }

  @Test
  public void testValidConnectionConfs() {
    testConnectionConfs(validConnectionConfs, true);
  }

  @Test
  public void testInvalidConnectionConfs() {
    testConnectionConfs(invalidConnectionConfs, false);
  }
}
