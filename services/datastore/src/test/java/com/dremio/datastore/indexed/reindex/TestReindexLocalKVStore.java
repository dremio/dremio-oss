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

package com.dremio.datastore.indexed.reindex;

import com.dremio.datastore.CustomLocalKVStoreProvider;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.indexed.doughnut.DoughnutIndexedStore;
import com.dremio.datastore.indexed.doughnut.UpgradedDoughnutStoreCreator;
import java.io.File;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

public class TestReindexLocalKVStore extends AbstractReindexTestKVStore {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private String temporaryPath1;

  private KVStoreProvider localKVStoreProvider;
  private KVStoreProvider upgradedLocalKVStoreProvider;

  @Override
  protected KVStoreProvider createKVStoreProvider() throws Exception {
    temporaryPath1 = TEMPORARY_FOLDER.newFolder().getPath();
    localKVStoreProvider =
        new CustomLocalKVStoreProvider(
            Collections.singleton(DoughnutIndexedStore.class), temporaryPath1, false, false);
    localKVStoreProvider.start();

    return localKVStoreProvider;
  }

  @Override
  protected KVStoreProvider createUpgradedKVStoreProvider() throws Exception {
    String temporaryPath2 = TEMPORARY_FOLDER.newFolder().getPath();
    FileUtils.copyDirectory(new File(temporaryPath1), new File(temporaryPath2));

    upgradedLocalKVStoreProvider =
        new CustomLocalKVStoreProvider(
            Collections.singleton(UpgradedDoughnutStoreCreator.class),
            temporaryPath2,
            false,
            false);
    upgradedLocalKVStoreProvider.start();

    return upgradedLocalKVStoreProvider;
  }

  @Override
  protected void closeProvider() throws Exception {
    localKVStoreProvider.close();
    upgradedLocalKVStoreProvider.close();
  }
}
