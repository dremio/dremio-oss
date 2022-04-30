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

import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterFactory;

public class DatastoreDatabaseAdapterFactory
    implements DatabaseAdapterFactory<NessieDatabaseAdapterConfig, NessieDatabaseAdapterConfig, NessieDatastoreInstance> {

  public static final String NAME = "DATASTORE";

  protected DatabaseAdapter create(NessieDatabaseAdapterConfig config, NessieDatastoreInstance connector) {
    return new DatastoreDatabaseAdapter(config, connector);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Builder<NessieDatabaseAdapterConfig, NessieDatabaseAdapterConfig, NessieDatastoreInstance>
      newBuilder() {
    return new DatastoreDatabaseAdapterBuilder();
  }

  private class DatastoreDatabaseAdapterBuilder
      extends Builder<NessieDatabaseAdapterConfig, NessieDatabaseAdapterConfig, NessieDatastoreInstance> {
    @Override
    protected NessieDatabaseAdapterConfig getDefaultConfig() {
      return new ImmutableNessieDatabaseAdapterConfig.Builder().build();
    }

    @Override
    protected NessieDatabaseAdapterConfig adjustableConfig(NessieDatabaseAdapterConfig config) {
      return new ImmutableNessieDatabaseAdapterConfig.Builder().from(config).build();
    }

    @Override
    public DatabaseAdapter build() {
      return create(getConfig(), getConnector());
    }
  }
}
