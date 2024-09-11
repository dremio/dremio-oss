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
package com.dremio.exec.server;

import com.dremio.common.exceptions.UserException;
import org.apache.arrow.memory.OutOfMemoryException;

/**
 * Node-level debug information, exposed down to individual operators (through the operator context)
 */
public interface NodeDebugContextProvider {
  /** no-op implementation of the NodeDebugContext */
  public static final NodeDebugContextProvider NOOP =
      new NodeDebugContextProvider() {

        @Override
        public void addMemoryContext(UserException.Builder exceptionBuilder) {}

        @Override
        public void addMemoryContext(
            UserException.Builder exceptionBuilder, OutOfMemoryException e) {}

        @Override
        public void addMemoryContext(UserException.Builder exceptionBuilder, Throwable e) {}

        @Override
        public void addHeapMemoryContext(UserException.Builder exceptionBuilder, Throwable e) {}

        @Override
        public void addErrorOrigin(UserException.Builder builder) {}

        @Override
        public void addErrorOrigin(UserException userException) {}
      };

  /**
   * Add information about the various allocators in the system. Useful when dealing with an OOM
   *
   * @param exceptionBuilder the exception (builder) that's about to get thrown
   */
  void addMemoryContext(UserException.Builder exceptionBuilder);

  void addMemoryContext(UserException.Builder exceptionBuilder, Throwable e);

  /**
   * Add information about the specific allocator that caused the failure. Useful when dealing with
   * an OOM
   *
   * @param exceptionBuilder the exception (builder) that's about to get thrown
   */
  void addMemoryContext(UserException.Builder exceptionBuilder, OutOfMemoryException e);

  void addHeapMemoryContext(UserException.Builder exceptionBuilder, Throwable e);

  public void addErrorOrigin(UserException.Builder builder);

  public void addErrorOrigin(UserException userException);
}
