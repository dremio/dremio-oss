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
package com.dremio.exec.store.hive.proxy;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Abstract class representing a Hive OrcScanFilter that is in a separate
 * ClassLoader from Dremio's.
 */
public abstract class HiveProxiedOrcScanFilter {

  private static final String JSON_PROP_KRYOBASE64ENCODEDFILTER = "kryoBase64EncodedFilter";
  private static final String COLUMN = "column";

  private final String kryoBase64EncodedFilter;
  private final SchemaPath column;

  protected HiveProxiedOrcScanFilter(String kryoBase64EncodedFilter, SchemaPath column) {
    this.kryoBase64EncodedFilter = kryoBase64EncodedFilter;
    this.column = column;
  }

  @JsonProperty(JSON_PROP_KRYOBASE64ENCODEDFILTER)
  public String getKryoBase64EncodedFilter() {
    return kryoBase64EncodedFilter;
  }

  @JsonProperty(COLUMN)
  public SchemaPath getColumn() {
    return column;
  }

  public abstract StoragePluginId getPluginId();

  public abstract double getCostAdjustment();

  @Override
  public abstract String toString();

  @Override
  public abstract boolean equals(Object o);

  @Override
  public abstract int hashCode();

}
