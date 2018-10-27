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
package com.dremio.exec.store.dfs;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.server.SabotContext;
import com.dremio.test.DremioTest;

/**
 * Unit tests for {@code HDFSStoragePlugin}
 */
public class TestHDFSStoragePlugin extends DremioTest {

  @Test
  public void testHdfsConfApplied() throws Exception {
    final HDFSConf conf = new HDFSConf();
    conf.hostname = "localhost";
    conf.shortCircuitFlag = HDFSConf.ShortCircuitFlag.ENABLED;
    conf.shortCircuitSocketPath = "/tmp/dn.sock";
    conf.propertyList = Arrays.asList(new Property("foo", "bar"));

    SabotContext context = mock(SabotContext.class);
    when(context.getClasspathScan()).thenReturn(DremioTest.CLASSPATH_SCAN_RESULT);

    Provider<StoragePluginId> idProvider = () -> { return new StoragePluginId(null, conf, null); };
    try(HDFSStoragePlugin fileSystemPlugin = new HDFSStoragePlugin(conf, context, "test-plugin", null, idProvider)) {
      fileSystemPlugin.start();

      final Configuration fsConf = fileSystemPlugin.getFsConf();
      assertThat(fsConf.get("dfs.client.read.shortcircuit"), is("true"));
      assertThat(fsConf.get("dfs.domain.socket.path"), is("/tmp/dn.sock"));
      assertThat(fsConf.get("foo"), is("bar"));
    }
  }

}
