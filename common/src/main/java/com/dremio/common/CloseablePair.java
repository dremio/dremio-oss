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

package com.dremio.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Closeable pair
 */
public class CloseablePair<K, V> implements AutoCloseable {
    private final K k;
    private final V v;

    public CloseablePair(K k, V v) {
        this.k = k;
        this.v = v;
    }

    public K getLeft() {
        return this.k;
    }

    public V getRight() {
        return this.v;
    }

    @Override
    public void close() throws Exception {
        final List<AutoCloseable> closeables = new ArrayList<>(2);
        if (k != null && k instanceof AutoCloseable) {
            closeables.add((AutoCloseable) k);
        }
        if (v != null && v instanceof AutoCloseable) {
            closeables.add((AutoCloseable) v);
        }
        AutoCloseables.close(closeables);
    }
}