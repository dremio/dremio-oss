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
package com.dremio.service.nessie;

import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;

/**
 * Nessie datastore database adapter config
 */
public class NessieDatabaseAdapterConfig implements NonTransactionalDatabaseAdapterConfig {

  private final int commitRetries;

  NessieDatabaseAdapterConfig(int commitRetries) {
    this.commitRetries = commitRetries;
  }

  @Override
  public int getCommitRetries() {
    return commitRetries;
  }
}
