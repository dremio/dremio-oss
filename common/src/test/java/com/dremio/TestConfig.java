/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.config.DremioConfig;
import com.typesafe.config.ConfigFactory;

/**
 * Test Dremio Config.
 */
public class TestConfig {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void initialize() {
    DremioConfig config = DremioConfig.create();
  }

  @Test
  public void fileOverride() {

    try(
        ResetProp prop1 = new ResetProp(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_BOOL);
        ResetProp prop2 = new ResetProp(DremioConfig.LOCAL_WRITE_PATH_STRING);
        ResetProp prop3 = new ResetProp(DremioConfig.DB_PATH_STRING);
        ResetProp prop4 = new ResetProp("dremd.write");
        ) {
      // clear relevant parameters.
      prop1.set(null);
      prop2.set(null);
      prop3.set(null);
      prop4.set(null);
      DremioConfig config = DremioConfig.create(getClass().getResource("/test-dremio.conf"));

      // check simple setting
      assertEquals(false, config.getBoolean(DremioConfig.EMBEDDED_MASTER_ZK_ENABLED_BOOL));

      // check overriden setting that uses
      assertEquals("/tmp/crazydir/db", config.getString(DremioConfig.DB_PATH_STRING));
    }
  }

  /**
   * Make sure that we're overriding options provided in a user config even if
   * that user config doesn't set that value.
   */
  @Test
  public void systemOverFile(){
    try(
        ResetProp prop1 = new ResetProp(DremioConfig.DB_PATH_STRING);
        ) {
      // clear relevant parameters.
      final String path = "my.fave.path";
      prop1.set(path);

      DremioConfig config = DremioConfig.create(getClass().getResource("/test-dremio.conf"));

      // check simple setting
      assertEquals(path, config.getString(DremioConfig.DB_PATH_STRING));
    }
  }

  @Test
  public void arrayProperties() throws Exception {
    DremioConfig config = DremioConfig.create(getClass().getResource("/test-dremio3.conf"));
    String property = (config.getStringList(DremioConfig.SPILLING_PATH_STRING)).toString();
    System.out.println(property);
    try(
      ResetProp prop1 = new ResetProp(DremioConfig.SPILLING_PATH_STRING);
    ) {
      // clear relevant parameters.
      final String path = property;
      prop1.set(path);
      DremioConfig configNew = DremioConfig.create(getClass().getResource("/test-dremio.conf"));
      assertEquals(path, configNew.getStringList(DremioConfig.SPILLING_PATH_STRING).toString());
      assertEquals(config.getStringList(DremioConfig.SPILLING_PATH_STRING), configNew.getStringList(DremioConfig
        .SPILLING_PATH_STRING));
    }
  }

  @Test
  public void badProperty() {
    exception.expect(RuntimeException.class);
    exception.expectMessage("mistyped-property");
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
    try(ResetProp prop = new ResetProp(name)) {
      prop.set("true");
      assertEquals(true, DremioConfig.create().getBoolean(DremioConfig.DEBUG_AUTOPORT_BOOL));
    }
  }


  @Test
  public void newSystemProp() {
    String name = DremioConfig.DEBUG_AUTOPORT_BOOL;

    try(ResetProp prop = new ResetProp(name)) {

      // set setting one way so we make sure we don't depend on external settings.
      prop.set("false");
      assertEquals(false, DremioConfig.create().getBoolean(name));

      // now set the value the other way, to make sure we're actually changing something.
      prop.set("true");
      assertEquals(true, DremioConfig.create().getBoolean(name));

    }
  }

  @Test
  public void newSystemPropWithDependency() {
    String name = DremioConfig.LOCAL_WRITE_PATH_STRING;

    try(ResetProp prop = new ResetProp(name)) {
      // set setting one way so we make sure we don't depend on external settings.
      prop.set("my.special.path");
      assertEquals("my.special.path/db", DremioConfig.create().getString(DremioConfig.DB_PATH_STRING));
    }
  }

  private class ResetProp implements AutoCloseable {

    private final String original;
    private final String property;

    public ResetProp(String property){
      this.property = property;
      this.original = System.getProperty(property);
    }

    public void set(String value){
      if(value == null){
        System.clearProperty(property);
      } else {
        System.setProperty(property, value);
      }
      ConfigFactory.invalidateCaches();
    }

    @Override
    public void close() {
      set(original);
      ConfigFactory.invalidateCaches();
    }

  }
}
