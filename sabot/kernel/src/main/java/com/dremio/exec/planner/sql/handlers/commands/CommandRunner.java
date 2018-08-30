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
package com.dremio.exec.planner.sql.handlers.commands;

public interface CommandRunner<T> {

  enum CommandType {
    /**
     * A command that runs on the coordination node and returns a response via a custom rpc message.
     */
    SYNC_RESPONSE,

    /**
     * A command that runs on the coordination node and returns a response via query data and result.
     */
    SYNC_QUERY,

    /**
     * A command that runs on one or more execution nodes and returns response via query data and result.
     */
    ASYNC_QUERY

  }

  /**
   * Plan until cost can be determined for query.
   * @return
   * @throws Exception
   */
  double plan() throws Exception;

  /**
   * Release any resources
   * @throws Exception
   */
  void close() throws Exception;
  /**
   * Finish planning and start execution.
   * @throws Exception
   */
  T execute() throws Exception;

  CommandType getCommandType();

  String getDescription();

}
