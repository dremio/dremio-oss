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
package com.dremio.exec.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.dremio.common.AutoCloseables;
import com.dremio.common.util.CloseableIterator;
import com.google.common.collect.Iterators;

/**
 * A {@link CloseableIterator} which concats multiple iterators and closes Autocloseables among them
 *
 * @param <T>
 */
public class ConcatenatedCloseableIterator<T> implements CloseableIterator<T> {

    private final Iterator<T> delegate;
    private final List<AutoCloseable> closeables;

    public static <K> CloseableIterator<K> of(Iterator<K>... inputs) {
        return new ConcatenatedCloseableIterator<>(inputs);
    }

    private ConcatenatedCloseableIterator(Iterator<T>... inputs) {
        closeables = Arrays.stream(inputs)
                .filter(AutoCloseable.class::isInstance)
                .map(AutoCloseable.class::cast)
                .collect(Collectors.toList());
        delegate = Iterators.concat(inputs);
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public T next() {
        return delegate.next();
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(closeables);
    }
}
