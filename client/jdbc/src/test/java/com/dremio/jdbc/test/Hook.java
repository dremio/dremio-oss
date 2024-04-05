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
package com.dremio.jdbc.test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public enum Hook {
  /** Called with the logical plan. */
  LOGICAL_PLAN;

  private final List<Consumer<Object>> handlers = new CopyOnWriteArrayList<>();

  public Closeable add(final Consumer handler) {
    handlers.add(handler);
    return () -> remove(handler);
  }

  /** Removes a handler from this Hook. */
  private boolean remove(Consumer handler) {
    return handlers.remove(handler);
  }

  /** Runs all handlers registered for this Hook, with the given argument. */
  public void run(Object arg) {
    for (Consumer<Object> handler : handlers) {
      handler.accept(arg);
    }
  }

  /** Removes a Hook after use. */
  public interface Closeable extends AutoCloseable {
    @Override
    void close(); // override, removing "throws"
  }
}
