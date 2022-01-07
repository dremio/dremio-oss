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
import com.dremio.service.Service;

/**
 * interface for creating TransientStores
 */
public interface TransientStoreProvider extends Service {

  /**
   * returns a TransientStore implementation for given parameters
   * @param keyFormat Format of key in the transient store
   * @param valueFormat Format of value in the transient store
   * @param <K> Key type
   * @param <V> Value type
   * @param <T> return type
   * @return TransientStore object
   */
  <K, V, T extends TransientStore<K, V>> T getStore(Format<K> keyFormat, Format<V> valueFormat);

  /**
   * returns a TransientStore implementation for given parameters
   * @param keyFormat Format of key in the transient store
   * @param valueFormat Format of value in the transient store
   * @param <K> Key type
   * @param <V> Value type
   * @param ttl time to live, in seconds, after which entries will be expired if there has been no access.
   * @param <T> return type
   * @return TransientStore object
   */
  <K, V, T extends TransientStore<K, V>> T getStore(Format<K> keyFormat, Format<V> valueFormat, int ttl);

}
