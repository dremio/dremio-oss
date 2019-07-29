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
package com.dremio.config;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.test.TemporarySystemProperties;

/**
 * Test Dremio Config.
 */
public class TestDremioConfig {

  @Rule
  public final TemporarySystemProperties properties = new TemporarySystemProperties();

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void initialize() {
    DremioConfig config = DremioConfig.create();
  }

  @Test
  public void fileOverride() {
    // clear relevant parameters.
    properties.clear(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_BOOL);
    properties.clear(DremioConfig.LOCAL_WRITE_PATH_STRING);
    properties.clear(DremioConfig.DB_PATH_STRING);
    properties.clear("dremd.write");

    DremioConfig config = DremioConfig.create(getClass().getResource("/test-dremio.conf"));

    // check simple setting
    assertEquals(false, config.getBoolean(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_BOOL));

    // check overriden setting that uses
    assertEquals("/tmp/crazydir/db", config.getString(DremioConfig.DB_PATH_STRING));
    assertEquals("pdfs:///tmp/crazydir/pdfs/accelerator", config.getString(DremioConfig.ACCELERATOR_PATH_STRING));
  }

  @Test
  public void distOverride() {
    // clear relevant parameters.
    properties.clear(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_BOOL);
    properties.clear(DremioConfig.LOCAL_WRITE_PATH_STRING);
    properties.clear(DremioConfig.DB_PATH_STRING);
    properties.clear("dremd.write");

    DremioConfig config = DremioConfig.create(getClass().getResource("/test-dremio4.conf"));

    // check overriden setting that uses
    assertEquals("/tmp/foobar/db", config.getString(DremioConfig.DB_PATH_STRING));
    assertEquals("pdfs:///tmp/foobar/dist", config.getString(DremioConfig.DIST_WRITE_PATH_STRING));
    assertEquals("pdfs:///tmp/foobar/dist/accelerator", config.getString(DremioConfig.ACCELERATOR_PATH_STRING));
  }

  /**
   * Make sure that we're overriding options provided in a user config even if
   * that user config doesn't set that value.
   */
  @Test
  public void systemOverFile(){
    // clear relevant parameters.
    final String path = "my.fave.path";
    properties.set(DremioConfig.DB_PATH_STRING, path);

    DremioConfig config = DremioConfig.create(getClass().getResource("/test-dremio.conf"));

    // check simple setting
    assertEquals(path, config.getString(DremioConfig.DB_PATH_STRING));
  }


  @Test
  public void arrayProperties() throws Exception {
    DremioConfig config = DremioConfig.create(getClass().getResource("/test-dremio3.conf"));
    String property = (config.getStringList(DremioConfig.SPILLING_PATH_STRING)).toString();
    // clear relevant parameters.
    final String path = property;
    properties.set(DremioConfig.SPILLING_PATH_STRING, path);
    DremioConfig configNew = DremioConfig.create(getClass().getResource("/test-dremio.conf"));
    assertEquals(path, configNew.getStringList(DremioConfig.SPILLING_PATH_STRING).toString());
    assertEquals(config.getStringList(DremioConfig.SPILLING_PATH_STRING), configNew.getStringList(DremioConfig
        .SPILLING_PATH_STRING));
  }

  @Test
  public void badProperty() {
    exception.expect(RuntimeException.class);
    exception.expectMessage("mistyped-property");
    @SuppressWarnings("unused")
    DremioConfig config = DremioConfig.create(getClass().getResource("/test-dremio2.conf"));
  }

  @Test
  public void appOverride() {

    DremioConfig config = DremioConfig.create()
        .withValue(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_BOOL, false)
        .withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, "/tmp/crazydir");

    // check simple setting
    assertEquals(false, config.getBoolean(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_BOOL));

    // check overriden setting

    assertEquals("/tmp/crazydir/db", config.getString(DremioConfig.DB_PATH_STRING));
  }

  @Test
  public void legacySystemProp() {
    String name = "dremio_autoPort";

    properties.set(name, "true");
    assertEquals(true, DremioConfig.create().getBoolean(DremioConfig.DEBUG_AUTOPORT_BOOL));
  }


  @Test
  public void newSystemProp() {
    String name = DremioConfig.DEBUG_AUTOPORT_BOOL;

    // set setting one way so we make sure we don't depend on external settings.
    properties.set(name, "false");
    assertEquals(false, DremioConfig.create().getBoolean(name));

    // now set the value the other way, to make sure we're actually changing something.
    properties.set(name, "true");
    assertEquals(true, DremioConfig.create().getBoolean(name));
  }

  @Test
  public void newSystemPropWithDependency() {
    String name = DremioConfig.LOCAL_WRITE_PATH_STRING;

    // set setting one way so we make sure we don't depend on external settings.
    properties.set(name, "my.special.path");
    assertEquals("my.special.path/db", DremioConfig.create().getString(DremioConfig.DB_PATH_STRING));
  }
}
