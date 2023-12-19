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

import java.io.IOException;

/**
 * This interface represents an error handler for processing and committing error information of type T.
 * Implementations of this interface should define the logic for processing the error information and
 * committing it after processing.
 *
 * @param <T> The type of error information that this error handler can handle, which must extend the ErrorInfo interface.
 */
public interface ErrorHandler<T extends ErrorInfo> {

  /**
   * Commit the processed error information. Implementations should define the logic for committing
   * the processed error information after processing.
   *
   * @throws IOException If an I/O error occurs during the commit operation.
   */
  void commit() throws Exception;

  /**
   * Process the given error information. Implementations should define the logic for processing the
   * error information.
   *
   * @param errorInfo The error information of type T to be processed.
   *
   * @throws IOException If an I/O error occurs during the process operation.
   */
  void process(T errorInfo) throws Exception;

}
