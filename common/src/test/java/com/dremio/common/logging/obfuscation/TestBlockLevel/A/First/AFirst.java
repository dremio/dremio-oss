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
package com.dremio.common.logging.obfuscation.TestBlockLevel.A.First;

import java.util.List;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

/**
 * class for testing custom log filtering
 */
public class AFirst {
  private static ch.qos.logback.classic.Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(AFirst.class);

  public List<ILoggingEvent> testLogFiltering() {
    ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
    listAppender.start();
    logger.addAppender(listAppender);
    logger.info("info message");
    logger.debug("debug message");
    logger.error("error message");
    logger.warn("warn message");
    logger.trace("trace message");
    List<ILoggingEvent> logsList = listAppender.list;
    return logsList;
  }
}
