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
package com.dremio.datastore.indexed.doughnut;

import static com.dremio.datastore.indexed.doughnut.DoughnutIndexKeys.NAME;
import static com.dremio.datastore.indexed.doughnut.DoughnutIndexKeys.PRICE;
import static com.dremio.datastore.indexed.doughnut.DoughnutIndexKeys.THICKNESS;

import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.IndexedStoreCreationFunction;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.format.Format;

/**
 * StoreCreator for upgraded doughnut
 */
public class UpgradedDoughnutStoreCreator implements IndexedStoreCreationFunction<String, Doughnut> {
  //We will need to address how to exclude packages from class path scanning. Tracking tkt is DX49216
  public static final String UPGRADED_DOUGHNUT_COLLECTION_NAME = "test-upgraded-doughnut-indexed-store";

  @Override
  public IndexedStore<String, Doughnut> build(StoreBuildingFactory factory) {
    return factory.<String, Doughnut>newStore()
      .name(UPGRADED_DOUGHNUT_COLLECTION_NAME)
      .keyFormat(Format.ofString())
      .valueFormat(Format.wrapped(Doughnut.class, new DoughnutConverter(), Format.ofBytes()))
      .buildIndexed(new UpgradedDoughnutDocumentConverter());
  }

  /**
   * DocumentConveter which removes index on flavor
   */
  public static class UpgradedDoughnutDocumentConverter extends DoughnutDocumentConverter {
    private Integer version = super.getVersion() + 1;
    // removed flavor
    @Override
    public void convert(DocumentWriter writer, String key, Doughnut record) {
      writer.write(NAME, record.getName());
      writer.write(PRICE, record.getPrice());
      writer.write(THICKNESS, record.getThickness());
    }
    @Override
    public Integer getVersion() {
      return version;
    }

  }
}
