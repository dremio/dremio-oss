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
package org.apache.iceberg.rest;

import com.dremio.exec.store.iceberg.DremioFileIO;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/** Currently wrapping for DremioFileIO only. */
public class DremioRESTTableOperations implements TableOperations {

  private final DremioFileIO dremioFileIO;
  private final RESTTableOperations delegate;

  public DremioRESTTableOperations(DremioFileIO dremioFileIO, TableOperations delegate) {
    this.dremioFileIO = dremioFileIO;
    this.delegate = (RESTTableOperations) delegate;
  }

  @Override
  public TableMetadata current() {
    return delegate.current();
  }

  @Override
  public TableMetadata refresh() {
    return delegate.refresh();
  }

  @Override
  public void commit(TableMetadata tableMetadata, TableMetadata tableMetadata1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileIO io() {
    return dremioFileIO;
  }

  @Override
  public String metadataFileLocation(String s) {
    return delegate.metadataFileLocation(s);
  }

  @Override
  public LocationProvider locationProvider() {
    return delegate.locationProvider();
  }
}
