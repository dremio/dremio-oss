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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

public class HiveTestUtilities {

  /**
   * Execute the give <i>query</i> on given <i>hiveDriver</i> instance. If a {@link
   * CommandNeedRetryException} exception is thrown, it tries upto 3 times before returning failure.
   *
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
    } catch (CommandNeedRetryException retryEx) {
      if (--retryCount == 0) {
        failed = true;
        cause = retryEx;
      }
    } catch (Exception ex) {
      failed = true;
      cause = ex;
    }

    if (failed || response.getResponseCode() != 0) {
      throw new RuntimeException(
          String.format(
              "Failed to execute command '%s', errorMsg = '%s'",
              query, (response != null ? response.getErrorMessage() : "")),
          cause);
    }
  }

  public static class DriverState implements AutoCloseable {

    // Hive data generation will invoke HS2 functionality, creating a SessionState (a Hive class),
    // which in turn creates
    // an UDFClassLoader instance. This will be set as context CL, but sadly upon
    // SessionState.close() it is not reset...
    private static final ClassLoader ORIGINAL_CLASS_LOADER =
        Thread.currentThread().getContextClassLoader();
    public final Driver driver;
    public final SessionState sessionState;

    public DriverState(HiveConf conf) {
      this.driver = new Driver(conf);
      this.sessionState = new SessionState(conf);
      SessionState.start(sessionState);
    }

    @Override
    public void close() throws IOException {
      sessionState.close();
      Thread.currentThread().setContextClassLoader(ORIGINAL_CLASS_LOADER);
    }
  }

  public static void logVersion(Driver hiveDriver) throws Exception {
    hiveDriver.run("SELECT VERSION()");
    hiveDriver.resetFetch();
    hiveDriver.setMaxRows(1);
    List<String> result = new ArrayList<>();
    hiveDriver.getResults(result);

    for (String values : result) {
      System.out.println("Test Hive instance version: " + values);
    }
  }

  public static void pingHive(Driver hiveDriver) throws Exception {
    executeQuery(hiveDriver, "CREATE DATABASE db_ping");
  }
}
