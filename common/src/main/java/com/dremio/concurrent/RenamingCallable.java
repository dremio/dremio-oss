/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.concurrent;

import java.util.concurrent.Callable;

/**
 * A generic callable that renames thread for debugging convenience.
 *
 * @param <V> value to return
 */
public final class RenamingCallable<V> implements Callable<V> {
  private final Callable<V> delegate;
  private final String name;

  private RenamingCallable(final Callable<V> delegate, final String name) {
    this.delegate = delegate;
    this.name = name;
  }

  @Override
  public V call() throws Exception {
    final Thread current = Thread.currentThread();
    final String originalName = current.getName();
    try {
      current.setName(name);
      return delegate.call();
    } finally {
      current.setName(originalName);
    }
  }

  public static <V> RenamingCallable<V> of(final Callable<V> delegate, final String name) {
    return new RenamingCallable<>(delegate, name);
  }

  public static RenamingCallable<Void> of(final Runnable delegate, final String name) {
    return of(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        delegate.run();
        return null;
      }
    }, name);
  }
}
