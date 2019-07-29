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
package com.dremio.common.concurrent;

import com.dremio.common.util.Closeable;
import com.google.common.base.Preconditions;

/**
 * A holder of an object that also has an autocloseable lock.
 *
 * @param <T> The value type.
 */
public class WithAutoCloseableLock<T> implements Closeable {

  private final AutoCloseableLock acl;
  private final T value;

  public WithAutoCloseableLock(AutoCloseableLock acl, T value) {
    super();
    this.acl = Preconditions.checkNotNull(acl);
    this.value = Preconditions.checkNotNull(value);
  }

  @Override
  public void close() {
    acl.close();
  }

  /**
   * Get the value that is included with the lock.
   * @return Value of type T.
   */
  public T getValue() {
    return value;
  }

}
