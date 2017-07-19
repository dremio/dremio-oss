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
package com.dremio.provision.yarn;

import java.text.MessageFormat;

import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.api.logging.LogThrowable;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;

/**
 * Custom Log converter from Twill Log Events to current logger
 */
public class YarnTwillLogHandler implements LogHandler {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(YarnTwillLogHandler.class);

  private static final ThreadLocal<MessageFormat> MESSAGE_FORMAT = new ThreadLocal<MessageFormat>() {
    @Override
    protected MessageFormat initialValue() {
      MessageFormat format = new MessageFormat("[{0}] {1}:{2}({3}:{4}) - {5}");
      return format;
    }
  };

  public YarnTwillLogHandler() {
  }

  @Override
  public void onLog(LogEntry logEntry) {

    final LoggingEvent loggingEvent = new LoggingEvent();
    loggingEvent.setTimeStamp(logEntry.getTimestamp());
    loggingEvent.setLoggerName(logEntry.getLoggerName());
    loggingEvent.setLevel(Level.valueOf(logEntry.getLogLevel().name()));
    loggingEvent.setThreadName(logEntry.getThreadName());
    Object [] formatObjects = new Object[] {logEntry.getHost(),
      getSimpleClassName(logEntry.getSourceClassName()),
      logEntry.getSourceMethodName(),
      logEntry.getFileName(),
      logEntry.getLineNumber(),logEntry.getMessage()};
    loggingEvent.setMessage(MESSAGE_FORMAT.get().format(formatObjects));

    // Prints the throwable and stack trace.
    LogThrowable logThrowable = logEntry.getThrowable();
    if (logThrowable != null) {
      loggingEvent.setThrowableProxy(new ThrowableProxy(setThrowable(logThrowable)));
    }

    if (logger instanceof Logger) {
      ((Logger) logger).callAppenders(loggingEvent);
    } else {
      logger.info("Logger is not instance of ch.qos.logback.classic.Logger. Logger event is: {}", loggingEvent);
    }
  }

  private Throwable setThrowable(LogThrowable logThrowable) {
    if (logThrowable == null) {
      return null;
    }
    Throwable throwable = new Throwable(logThrowable.getMessage());
    throwable.setStackTrace(logThrowable.getStackTraces());
    throwable.initCause(setThrowable(logThrowable.getCause()));
    return throwable;
  }

  private static String getSimpleClassName(String className) {
    return className.substring(className.lastIndexOf('.') + 1);
  }
}
