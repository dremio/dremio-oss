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

import java.util.Iterator;

/**
 * Listing of {@link DatasetHandle dataset handles}. Implementations typically hold onto resources and state to be able
 * to provide the next {@link DatasetHandle}. When the listing is no longer used, the callers will invoke {@link #close}.
 * Implementations must release any held resources, and clear the internal state.
 */
public interface DatasetHandleListing extends AutoCloseable {

  /**
   * Return an iterator of dataset handles.
   *
   * @return iterator of dataset handles
   */
  Iterator<? extends DatasetHandle> iterator();

  /**
   * Release any resources; this listing will not be used after this call.
   */
  @Override
  default void close() {
  }

}
