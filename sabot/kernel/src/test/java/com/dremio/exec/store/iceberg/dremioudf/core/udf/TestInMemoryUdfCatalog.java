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
package com.dremio.exec.store.iceberg.dremioudf.core.udf;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.BeforeEach;

public class TestInMemoryUdfCatalog extends UdfCatalogTests<InMemoryCatalog> {
  private InMemoryCatalog catalog;

  @BeforeEach
  public void before() {
    this.catalog = new InMemoryCatalog();
    this.catalog.initialize("in-memory-catalog", ImmutableMap.of());
  }

  @Override
  protected InMemoryCatalog catalog() {
    return catalog;
  }

  @Override
  protected Catalog tableCatalog() {
    return catalog;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }
}
