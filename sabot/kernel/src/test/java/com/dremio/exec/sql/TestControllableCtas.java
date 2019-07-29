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
package com.dremio.exec.sql;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.source.proto.SourceConfig;

public class TestControllableCtas extends BaseTestQuery {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void addWritableSource() throws IOException {
    NASConf writable = new NASConf();
    writable.allowCreateDrop = true;
    File folderWritable = folder.newFolder();

    // add an existing folder.
    new File(folderWritable, "existing").mkdirs();
    writable.path = folderWritable.getCanonicalPath();
    SourceConfig conf = new SourceConfig();
    conf.setConnectionConf(writable);
    conf.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE);
    conf.setName("writable");
    getSabotContext().getCatalogService().createSourceIfMissingWithThrow(conf);
  }


  @Before
  public void addNonWritableSource() throws IOException {
    NASConf writable = new NASConf();
    writable.allowCreateDrop = false;
    File folderWritable = folder.newFolder();
    writable.path = folderWritable.getCanonicalPath();
    SourceConfig conf = new SourceConfig();
    conf.setConnectionConf(writable);
    conf.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE);
    conf.setName("nonwritable");
    getSabotContext().getCatalogService().createSourceIfMissingWithThrow(conf);
  }

  @After
  public void removeSources() {
    ( (CatalogServiceImpl) getSabotContext().getCatalogService()).deleteSource("writable");
    ( (CatalogServiceImpl) getSabotContext().getCatalogService()).deleteSource("nonwritable");
  }

  @Test
  public void ensureFailOnDisabledCtas() throws Exception {
    thrown.expectMessage("Unable to create table");
    test("create table nonwritable.mytable as select 1");
    thrown.expectMessage("Unable to drop table");
    test("drop table nonwritable.mytable");
  }

  @Test
  public void ensureSucceedOnEnabledCtas() throws Exception {
    test("create table writable.mytable as select 1");
    test("drop table writable.mytable");

  }

  @Test
  public void ensureExistingDirectoryFails() throws Exception {
    thrown.expectMessage("Folder already exists at path");
    test("create table writable.existing as select 1");
  }

  @Test
  public void ensureSubdirectoryOfExistingDirectorySucceeeds() throws Exception {
    test("create table writable.existing.subdirectory as select 1");
  }
}
