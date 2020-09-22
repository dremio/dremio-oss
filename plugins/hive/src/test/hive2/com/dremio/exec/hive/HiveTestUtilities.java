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
package com.dremio.exec.hive;

import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

public class HiveTestUtilities {

  /**
   * Execute the give <i>query</i> on given <i>hiveDriver</i> instance. If a {@link CommandNeedRetryException}
   * exception is thrown, it tries upto 3 times before returning failure.
   * @param hiveDriver
   * @param query
   */
  public static void executeQuery(Driver hiveDriver, String query) {
    CommandProcessorResponse response = null;
    boolean failed = false;
    int retryCount = 3;

    Exception cause = null;
    try {
      response = hiveDriver.run(query);
    } catch(CommandNeedRetryException retryEx) {
      if (--retryCount == 0) {
        failed = true;
        cause = retryEx;
      }
    } catch (Exception ex) {
      failed = true;
      cause = ex;
    }

    if (failed || response.getResponseCode() != 0 ) {
      throw new RuntimeException(String.format("Failed to execute command '%s', errorMsg = '%s'",
          query, (response != null ? response.getErrorMessage() : "")), cause);
    }
  }
}
