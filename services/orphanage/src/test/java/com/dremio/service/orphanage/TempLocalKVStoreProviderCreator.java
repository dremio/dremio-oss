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
package com.dremio.service.orphanage;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.KVStoreProvider;
import java.io.IOException;
import org.junit.rules.TemporaryFolder;

/** Creates a LocalKVStoreProvider off a temporary folder. */
public final class TempLocalKVStoreProviderCreator {
  public static final SabotConfig DEFAULT_SABOT_CONFIG = SabotConfig.forClient();
  public static final ScanResult CLASSPATH_SCAN_RESULT =
      ClassPathScanner.fromPrescan(DEFAULT_SABOT_CONFIG);
  private final TemporaryFolder kvFolder = new TemporaryFolder();
  private KVStoreProvider kvStoreProvider;

  private TempLocalKVStoreProviderCreator() throws IOException {
    kvFolder.create();
    kvStoreProvider =
        new LocalKVStoreProvider(
            CLASSPATH_SCAN_RESULT,
            kvFolder.newFolder().getPath(),
            true /*inMemory*/,
            false /*timed*/);
  }

  public static KVStoreProvider create() {
    try {
      TempLocalKVStoreProviderCreator creator = new TempLocalKVStoreProviderCreator();
      creator.kvStoreProvider.start();
      return creator.kvStoreProvider;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
