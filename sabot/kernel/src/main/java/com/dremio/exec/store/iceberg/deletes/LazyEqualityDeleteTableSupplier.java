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
package com.dremio.exec.store.iceberg.deletes;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.SchemaPath;
import com.google.common.base.Preconditions;

/**
 * A closeable Supplier of {@link EqualityDeleteHashTable EqualityDeleteHashTable(s)} sourced from one or more
 * {@link EqualityDeleteFileReader} instances.  Hash tables will not be built until get() is called.
 */
@NotThreadSafe
public class LazyEqualityDeleteTableSupplier implements Supplier<List<EqualityDeleteHashTable>>, AutoCloseable {

  private final List<SchemaPath> allEqualityFields;
  private List<EqualityDeleteFileReader> readers;
  private List<EqualityDeleteHashTable> tables;

  public LazyEqualityDeleteTableSupplier(List<EqualityDeleteFileReader> readers) {
    this.readers = Preconditions.checkNotNull(readers);
    this.tables = null;
    this.allEqualityFields = readers.stream()
        .flatMap(r -> r.getEqualityFields().stream())
        .distinct()
        .collect(Collectors.toList());
  }

  @Override
  public List<EqualityDeleteHashTable> get() {
    Preconditions.checkState(readers != null || tables != null, "Instance has been closed");
    if (tables == null) {
      tables = readers.stream()
          .map(reader -> {
            try {
              return reader.buildHashTable();
            } finally {
              AutoCloseables.close(RuntimeException.class, reader);
            }
          })
          .collect(Collectors.toList());
      readers = null;
    }

    return tables;
  }

  public List<SchemaPath> getAllEqualityFields() {
    return allEqualityFields;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(readers);
    AutoCloseables.close(tables);
    readers = null;
    tables = null;
  }
}
