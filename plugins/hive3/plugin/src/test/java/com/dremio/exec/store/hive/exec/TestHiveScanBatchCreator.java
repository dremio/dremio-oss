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
package com.dremio.exec.store.hive.exec;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.store.hive.Hive3StoragePlugin;

public class TestHiveScanBatchCreator {
  @Test
  public void ensureStoragePluginIsUsedForUsername() throws Exception {
    final String originalName = "Test";
    final String finalName = "Replaced";

    final Hive3StoragePlugin plugin = mock(Hive3StoragePlugin.class);
    when(plugin.getUsername(originalName)).thenReturn(finalName);

    final OpProps props = mock(OpProps.class);
    when(props.getUserName()).thenReturn(originalName);

    final HiveScanBatchCreator creator = mock(HiveScanBatchCreator.class);
    when(creator.getUGI(any(), any())).thenCallRealMethod();

    final UserGroupInformation ugi = creator.getUGI(plugin, props);
    verify(plugin).getUsername(originalName);
    assertEquals(finalName, ugi.getUserName());
  }
}
