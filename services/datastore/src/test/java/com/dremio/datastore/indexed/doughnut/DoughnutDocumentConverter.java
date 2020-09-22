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

import static com.dremio.datastore.indexed.doughnut.DoughnutIndexKeys.DIAMETER;
import static com.dremio.datastore.indexed.doughnut.DoughnutIndexKeys.FLAVOR;
import static com.dremio.datastore.indexed.doughnut.DoughnutIndexKeys.NAME;
import static com.dremio.datastore.indexed.doughnut.DoughnutIndexKeys.PRICE;
import static com.dremio.datastore.indexed.doughnut.DoughnutIndexKeys.THICKNESS;

import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;

/**
 * Doughnut DocumentConverter implementation.
 * Used in IndexedStore tests.
 */
public class DoughnutDocumentConverter implements DocumentConverter<String, Doughnut> {
  @Override
  public void convert(DocumentWriter writer, String key, Doughnut record) {
    writer.write(NAME, record.getName());
    writer.write(FLAVOR, record.getFlavor());
    writer.write(PRICE, record.getPrice());
    writer.write(THICKNESS, record.getThickness());
    writer.write(DIAMETER, record.getDiameter());
  }
}
