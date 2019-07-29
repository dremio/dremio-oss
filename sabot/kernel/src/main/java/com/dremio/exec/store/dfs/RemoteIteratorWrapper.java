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
package com.dremio.exec.store.dfs;

import java.io.IOError;
import java.io.IOException;
import java.util.Iterator;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.fs.RemoteIterator;

/**
 * Iterator<T> wrapper for RemoteIterator
 * @param <T>
 * @throws IOError if underlying remote iterator throws IOException
 */
public class RemoteIteratorWrapper<T> implements Iterator<T> {
  private final RemoteIterator<T> baseIterator;

  public RemoteIteratorWrapper(@NotNull RemoteIterator<T> baseIterator) {
    this.baseIterator = baseIterator;
  }

  @Override
  public boolean hasNext() {
    try {
      return baseIterator.hasNext();
    } catch(IOException e) {
      throw new IOError(e);
    }
  }

  @Override
  public T next() {
    try {
      return baseIterator.next();
    } catch(IOException e) {
      throw new IOError(e);
    }
  }
}
