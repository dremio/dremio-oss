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
package com.dremio.exec.store.hive;

import com.dremio.common.exceptions.UserException;

public class HiveUtilities {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveUtilities.class);

  private static final String ERROR_MSG = "Unsupported Hive data type %s. \n"
      + "Following Hive data types are supported in Dremio for querying: "
      + "BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DATE, TIMESTAMP, BINARY, DECIMAL, STRING, VARCHAR and CHAR";

  public static void throwUnsupportedHiveDataTypeError(String unsupportedType) {
    throw UserException.unsupportedError()
        .message(ERROR_MSG, unsupportedType)
        .build(logger);
  }

}

