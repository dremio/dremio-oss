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
package com.dremio.common;

import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import org.slf4j.bridge.SLF4JBridgeHandler;

/** Helper to install SLF4J bridge for JUL and make sure everything is redirected correctly */
public final class JULBridge {
  // JUL LogManager uses weak references for Logger and does not preserve manual configuration
  // changes if the instance is garbage collected.
  // Keeping a reference to parquet root logger so that our config changes persist over time.
  private static final Logger PARQUET_ROOT_LOGGER = Logger.getLogger("org.apache.parquet");

  private JULBridge() {}

  public static void configure() {
    // Route JUL logging messages to SLF4J.
    LogManager.getLogManager().reset();
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();

    // Parquet installs a default handler if none found, so remove existing handlers and add slf4j
    // one.
    // Also add slf4j bridge handle and configure additivity to false so that log messages are not
    // printed out twice
    final Handler[] handlers = PARQUET_ROOT_LOGGER.getHandlers();
    for (int i = 0; i < handlers.length; i++) {
      PARQUET_ROOT_LOGGER.removeHandler(handlers[i]);
    }
    PARQUET_ROOT_LOGGER.addHandler(new SLF4JBridgeHandler());
    PARQUET_ROOT_LOGGER.setUseParentHandlers(false);
  }
}
