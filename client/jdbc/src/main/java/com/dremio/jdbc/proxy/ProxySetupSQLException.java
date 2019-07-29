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
package com.dremio.jdbc.proxy;

import java.sql.SQLNonTransientConnectionException;

/**
 * SQLException for tracing-proxy connection and setup problems.
 */
class ProxySetupSQLException extends SQLNonTransientConnectionException {

  private static final long serialVersionUID = 2015_02_08L;

  ProxySetupSQLException( String message, Throwable cause ) {
    super( message, cause );
  }

  ProxySetupSQLException( String message ) {
    super( message );
  }

} // class ProxySetupSQLException
