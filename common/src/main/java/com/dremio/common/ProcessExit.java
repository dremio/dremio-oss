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
package com.dremio.common;

import java.io.PrintStream;


/**
 * Helper for catastrophic failures
 */
public final class ProcessExit {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger("root");

  private ProcessExit() {
  }

  /**
   * Exit the VM as we hit a catastrophic failure.
   * @param message a descriptive message
   * @param code an error code to exit the JVM with.
   */
  public static void exit(String message, int code) {
    try {
      exit(null, message, code);
    } finally {
      // We tried to exit with a nice error message, but that failed for some reason. Can't let this thread simply exit
      Runtime.getRuntime().halt(code);
    }
  }

  public static void exitHeap(Throwable t, int code) {
    try {
      exit(t, "There was insufficient heap memory to continue operating.", code);
    } finally {
      // We tried to exit with a nice error message, but that failed for some reason. Can't let this thread simply exit
      Runtime.getRuntime().halt(code);
    }
  }

  /**
   * Exit the VM as we hit a catastrophic failure.
   * @param t
   *          The Throwable that occurred
   * @param message
   *          A descriptive message
   * @param code
   *          An error code to exit the JVM with.
   */
  public static void exit(Throwable t, String message, int code) {
    try {
      logger.error("Dremio is exiting. {}", message, t);

      final PrintStream out = ("true".equals(System.getProperty("dremio.catastrophic_to_standard_out", "true"))) ? System.out
        : System.err;
      out.println("Dremio is exiting. " + message);
      if (t != null) {
        t.printStackTrace(out);
      }
      out.flush();
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {}
    } finally {
      // We tried to exit with a nice error message, but that failed for some reason. Can't let this thread simply exit
      Runtime.getRuntime().halt(code);
    }
  }
}
