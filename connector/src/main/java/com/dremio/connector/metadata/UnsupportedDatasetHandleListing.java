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
package com.dremio.connector.metadata;

import java.util.Collections;
import java.util.Iterator;

/**
 * "Null object" listing of {@link DatasetHandle dataset handles} when the source does not support listing datasets.
 */
public final class UnsupportedDatasetHandleListing implements DatasetHandleListing {
  public static final UnsupportedDatasetHandleListing INSTANCE = new UnsupportedDatasetHandleListing();

  private UnsupportedDatasetHandleListing() {
  }

  @Override
  public Iterator<DatasetHandle> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public void close() {
  }
}
