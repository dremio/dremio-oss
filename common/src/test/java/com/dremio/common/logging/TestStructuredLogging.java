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
package com.dremio.common.logging;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.test.DremioTest;
import com.google.protobuf.util.JsonFormat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.FileAppender;
import net.logstash.logback.composite.JsonProvider;
import net.logstash.logback.composite.JsonProviders;
import net.logstash.logback.composite.loggingevent.ArgumentsJsonProvider;
import net.logstash.logback.composite.loggingevent.LogLevelJsonProvider;
import net.logstash.logback.composite.loggingevent.MessageJsonProvider;
import net.logstash.logback.encoder.LoggingEventCompositeJsonEncoder;

/**
 * Test structured logging.
 */
public class TestStructuredLogging extends DremioTest {
  private static final String LOGGER_NAME = "STRUCTURED-LOG-TEST";
  private LoggerContext localLoggerContext;

  @Rule
  public TemporaryFolder tempLogFolder = new TemporaryFolder();

  @Before
  public void initializeLogger() throws IOException {
    localLoggerContext = new LoggerContext();
    localLoggerContext.start();
    Logger logger = localLoggerContext.getLogger(LOGGER_NAME);
    logger.setLevel(Level.ALL);
  }

  @After
  public void cleanup() {
    localLoggerContext.stop();
  }

  private File addFileAppenderToLogger(Logger logbackLogger, JsonProvider...providers) throws IOException {
    FileAppender fileAppender = new FileAppender<>();
    File tempFile = tempLogFolder.newFile("structuredLogTest");
    fileAppender.setName("StructuredLogger");
    fileAppender.setFile(tempFile.getAbsolutePath());
    fileAppender.setContext(logbackLogger.getLoggerContext());
    fileAppender.setAppend(true);

    LoggingEventCompositeJsonEncoder encoder = new LoggingEventCompositeJsonEncoder();
    encoder.setContext(logbackLogger.getLoggerContext());
    encoder.setProviders(new JsonProviders<>());
    if (providers != null) {
      for (JsonProvider provider: providers) {
        encoder.getProviders().addProvider(provider);
      }
    }
    fileAppender.setEncoder(encoder);
    encoder.start();
    fileAppender.start();
    logbackLogger.addAppender(fileAppender);
    return tempFile;
  }

  @Test
  public void testStructuredLogging() throws IOException {
    File logFile = addFileAppenderToLogger(localLoggerContext.getLogger(LOGGER_NAME), new ArgumentsJsonProvider());

    ProtobufStructuredLogger<StructuredLogData> structuredLogger = new ProtobufStructuredLogger<>(localLoggerContext.getLogger(LOGGER_NAME));

    StructuredLogData.Builder builder = StructuredLogData.newBuilder();
    builder.setId("id-1");
    builder.setMessage("Log Data message");
    builder.setStart(System.currentTimeMillis());
    builder.setFinish(System.currentTimeMillis() + 1000);
    builder.setOptionalMessage("Optional Message");
    StructuredLogData expected = builder.build();

    structuredLogger.info("test log", expected);
    try(BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
      String jsonLine = reader.readLine();
      StructuredLogData.Builder actualBuilder = StructuredLogData.newBuilder();
      JsonFormat.parser().merge(jsonLine, actualBuilder);
      Assert.assertEquals("Verify deserialization", expected, actualBuilder.build());
    }
  }

  @Test
  public void testStructuredLoggingLogLevel() throws IOException {
    File logFile = addFileAppenderToLogger(localLoggerContext.getLogger(LOGGER_NAME), new LogLevelJsonProvider(), new MessageJsonProvider());

    ProtobufStructuredLogger<StructuredLogData> structuredLogger = new ProtobufStructuredLogger<>(localLoggerContext.getLogger(LOGGER_NAME));

    StructuredLogData.Builder builder = StructuredLogData.newBuilder();
    builder.setId("id-1");
    builder.setMessage("Log Data message");
    builder.setStart(System.currentTimeMillis());
    builder.setFinish(System.currentTimeMillis() + 1000);
    builder.setOptionalMessage("Optional Message");
    StructuredLogData logStruct = builder.build();

    structuredLogger.info("test log", logStruct);
    structuredLogger.debug("debug log", logStruct);
    try(BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
      String jsonLine = reader.readLine();
      Assert.assertEquals("Verify logLevel-Info", "{\"level\":\"INFO\",\"message\":\"test log\"}", jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals("Verify logLevel-Debug", "{\"level\":\"DEBUG\",\"message\":\"debug log\"}", jsonLine);
    }
  }
}
