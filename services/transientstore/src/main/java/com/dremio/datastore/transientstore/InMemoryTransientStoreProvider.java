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
package com.dremio.datastore.transientstore;

import com.dremio.datastore.format.Format;

/** main implementation class for creating in memory stores. */
public class InMemoryTransientStoreProvider implements TransientStoreProvider {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(InMemoryTransientStoreProvider.class);

  @Override
  public <K, V, T extends TransientStore<K, V>> T getStore(
      Format<K> keyFormat, Format<V> valueFormat) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <K, V, T extends TransientStore<K, V>> T getStore(
      Format<K> keyFormat, Format<V> valueFormat, int ttl) {
    return (T) new InMemoryTransientStore(ttl);
  }

  @Override
  public void start() throws Exception {
    logger.info("Started InMemoryTransientStoreProvider");
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopped InMemoryTransientStoreProvider");
  }
}
