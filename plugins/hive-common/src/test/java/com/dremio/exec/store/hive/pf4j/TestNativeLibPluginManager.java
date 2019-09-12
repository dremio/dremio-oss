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
package com.dremio.exec.store.hive.pf4j;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.config.DremioConfig;
import com.dremio.test.TemporarySystemProperties;

/**
 * Test {@code NativeLibPluginManager} class
 */
public class TestNativeLibPluginManager {

  private final NativeLibPluginManager nativeLibPluginManager = new NativeLibPluginManager();

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Test
  public void testShouldReturnPluginRoot() {
    // Given
    properties.set(DremioConfig.PLUGINS_ROOT_PATH_PROPERTY, "/tmp/plugins");
    Path expectedPath = Paths.get("/tmp/plugins/connectors");

    // when
    Path actualPath = nativeLibPluginManager.createPluginsRoot();

    // then
    Assert.assertEquals(expectedPath, actualPath);
  }
}
