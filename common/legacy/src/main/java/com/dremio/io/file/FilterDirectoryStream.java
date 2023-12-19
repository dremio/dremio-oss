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
package com.dremio.io.file;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.util.Iterator;
import java.util.Objects;

/**
 * Filters an existing DirectoryStream
 *
 * @param <T> the type of the streamed iterms
 */
public class FilterDirectoryStream<T> implements DirectoryStream<T> {
  private final DirectoryStream<T> delegate;

  public FilterDirectoryStream(DirectoryStream<T> delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public Iterator<T> iterator() {
    return delegate.iterator();
  }
}
