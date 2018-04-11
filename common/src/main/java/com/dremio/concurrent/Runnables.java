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

/**
 * Utility class to wrap runnables.
 */
public final class Runnables {

  private Runnables() {}

  public static Runnable combo(Runnable runnable) {
    return safe(singleton(named(runnable)));
  }

  public static Runnable singleton(Runnable runnable) {
    return new SingletonRunnable(runnable);
  }

  public static Runnable safe(Runnable runnable) {
    return new SafeRunnable(runnable);
  }

  public static Runnable named(Runnable runnable) {
    return RenamingRunnable.of(runnable, runnable.toString());
  }

  public static void executeInSeparateThread(final Runnable runnable) {
    new Thread() {
      @Override
      public void run() {
        runnable.run();
      }

    }.start();
  }
}
