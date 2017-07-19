/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.vector.accessor;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;

// ?????? TODO clean this up:
// - determine and document conditions
// - determine place in SQLException type hierarchy
// - (Why are seemingly JDBC-specific SqlAccessor classes in
//    com.dremio.exec.vector.accessor?  Can they be in
//    com.dremio.jdbc.impl?
//   Why are they in java-exec module?  Can they be in JDBC module?

public class InvalidAccessException extends SQLNonTransientException {
  private static final long serialVersionUID = 2015_04_07L;

  public InvalidAccessException( String message ) {
    super(message);
  }
}