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
package com.dremio.concurrent;

/**
 * A generic runnable that renames thread for debugging convenience.
 */
public final class RenamingRunnable implements Runnable {
  private final Runnable delegate;
  private final String name;

  private RenamingRunnable(final Runnable delegate, final String name) {
    this.delegate = delegate;
    this.name = name;
  }

  @Override
  public void run() {
    final Thread current = Thread.currentThread();
    final String originalName = current.getName();
    try {
      current.setName(name);
      delegate.run();
    } finally {
      current.setName(originalName);
    }
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  public static RenamingRunnable of(final Runnable delegate, final String name) {
    return new RenamingRunnable(delegate, name);
  }
}
