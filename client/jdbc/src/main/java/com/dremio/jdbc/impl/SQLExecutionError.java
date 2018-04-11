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
package com.dremio.jdbc.impl;

import java.sql.SQLException;

@SuppressWarnings("serial")
/**
 * Wrapper class for {@code SQLException} when Avatica
 * doesn't allow for the exception to be thrown directly.
 */
class SQLExecutionError extends Error {
  public SQLExecutionError(String message, SQLException cause) {
    super(message, cause);
    // TODO Auto-generated constructor stub
  }

  @Override
  public synchronized SQLException getCause() {
    return (SQLException) super.getCause();
  }

}
