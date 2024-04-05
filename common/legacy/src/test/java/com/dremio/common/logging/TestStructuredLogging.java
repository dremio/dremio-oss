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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.FileAppender;
import com.dremio.test.DremioTest;
import com.google.protobuf.util.JsonFormat;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import net.logstash.logback.argument.StructuredArguments;
import net.logstash.logback.composite.JsonProvider;
import net.logstash.logback.composite.JsonProviders;
import net.logstash.logback.composite.loggingevent.ArgumentsJsonProvider;
import net.logstash.logback.composite.loggingevent.LogLevelJsonProvider;
import net.logstash.logback.composite.loggingevent.MessageJsonProvider;
import net.logstash.logback.composite.loggingevent.StackTraceJsonProvider;
import net.logstash.logback.encoder.LoggingEventCompositeJsonEncoder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test structured logging. */
public class TestStructuredLogging extends DremioTest {
  private static final String LOGGER_NAME = "STRUCTURED-LOG-TEST";
  private LoggerContext localLoggerContext;

  @SuppressWarnings("Slf4jLoggerShouldBeFinal")
  private Logger logger;

  @Rule public TemporaryFolder tempLogFolder = new TemporaryFolder();

  @Before
  public void initializeLogger() throws IOException {
    localLoggerContext = new LoggerContext();
    localLoggerContext.start();
    logger = localLoggerContext.getLogger(LOGGER_NAME);
    logger.setLevel(Level.ALL);
  }

  @After
  public void cleanup() {
    localLoggerContext.stop();
  }

  private File addFileAppenderToLogger(Logger logbackLogger, JsonProvider... providers)
      throws IOException {
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
      for (JsonProvider provider : providers) {
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
    File logFile =
        addFileAppenderToLogger(
            localLoggerContext.getLogger(LOGGER_NAME), new ArgumentsJsonProvider());

    ProtobufStructuredLogger<StructuredLogData> structuredLogger =
        new ProtobufStructuredLogger<>(localLoggerContext.getLogger(LOGGER_NAME));

    StructuredLogData.Builder builder = StructuredLogData.newBuilder();
    builder.setId("id-1");
    builder.setMessage("Log Data message");
    builder.setStart(System.currentTimeMillis());
    builder.setFinish(System.currentTimeMillis() + 1000);
    builder.setOptionalMessage("Optional Message");
    StructuredLogData expected = builder.build();

    structuredLogger.info(expected, "test log");
    try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
      String jsonLine = reader.readLine();
      StructuredLogData.Builder actualBuilder = StructuredLogData.newBuilder();
      JsonFormat.parser().merge(jsonLine, actualBuilder);
      Assert.assertEquals("Verify deserialization", expected, actualBuilder.build());
    }
  }

  @Test
  public void testStructuredLoggingLogLevel() throws IOException {
    File logFile =
        addFileAppenderToLogger(
            localLoggerContext.getLogger(LOGGER_NAME),
            new LogLevelJsonProvider(),
            new MessageJsonProvider(),
            new StackTraceJsonProvider());

    ProtobufStructuredLogger<StructuredLogData> structuredLogger =
        new ProtobufStructuredLogger<>(localLoggerContext.getLogger(LOGGER_NAME));

    StructuredLogData.Builder builder = StructuredLogData.newBuilder();
    builder.setId("id-1");
    builder.setMessage("Log Data message");
    builder.setStart(System.currentTimeMillis());
    builder.setFinish(System.currentTimeMillis() + 1000);
    builder.setOptionalMessage("Optional Message");
    StructuredLogData logStruct = builder.build();

    structuredLogger.info(logStruct, "test log");
    structuredLogger.debug(logStruct, "debug log");
    structuredLogger.warn(logStruct, "warn log");
    structuredLogger.error(logStruct, "error log");
    structuredLogger.info(logStruct, "test log {}", "parameterized");
    structuredLogger.debug(logStruct, "debug log {}", "parameterized");
    structuredLogger.warn(logStruct, "warn log {}", "parameterized");
    structuredLogger.error(logStruct, "error log {}", "parameterized");

    structuredLogger.info(logStruct, "test log multiple parameter {} {}", "arg1", "arg2");
    structuredLogger.info(
        logStruct, "test log multiple parameter {} {} {}", "arg1", "arg2", "arg3");
    structuredLogger.info(
        logStruct, "test log multiple parameter {} {} {} {}", "arg1", "arg2", "arg3", "arg4");
    structuredLogger.error(logStruct, "error log multiple parameter {} {}", "arg1", "arg2");
    structuredLogger.error(
        logStruct, "error log multiple parameter {} {} {}", "arg1", "arg2", "arg3");
    structuredLogger.error(
        logStruct, "error log multiple parameter {} {} {} {}", "arg1", "arg2", "arg3", "arg4");

    structuredLogger.info(logStruct, "test log", new RuntimeException("error"));
    structuredLogger.debug(
        logStruct, "debug log {}", "parameterized", new RuntimeException("error"));
    structuredLogger.warn(
        logStruct,
        "warn log multiple parameter {} {} {}",
        "arg1",
        "arg2",
        "arg3",
        new RuntimeException("error"));
    structuredLogger.error(
        logStruct,
        "error log multiple parameter {} {} {} {}",
        "arg1",
        "arg2",
        "arg3",
        "arg4",
        new RuntimeException("error"));

    try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
      String jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Info", "{\"level\":\"INFO\",\"message\":\"test log\"}", jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Debug", "{\"level\":\"DEBUG\",\"message\":\"debug log\"}", jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Warn", "{\"level\":\"WARN\",\"message\":\"warn log\"}", jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Error", "{\"level\":\"ERROR\",\"message\":\"error log\"}", jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Info",
          "{\"level\":\"INFO\",\"message\":\"test log parameterized\"}",
          jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Debug",
          "{\"level\":\"DEBUG\",\"message\":\"debug log parameterized\"}",
          jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Warn",
          "{\"level\":\"WARN\",\"message\":\"warn log parameterized\"}",
          jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Error",
          "{\"level\":\"ERROR\",\"message\":\"error log parameterized\"}",
          jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Info",
          "{\"level\":\"INFO\",\"message\":\"test log multiple parameter arg1 arg2\"}",
          jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Info",
          "{\"level\":\"INFO\",\"message\":\"test log multiple parameter arg1 arg2 arg3\"}",
          jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Info",
          "{\"level\":\"INFO\",\"message\":\"test log multiple parameter arg1 arg2 arg3 arg4\"}",
          jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Error",
          "{\"level\":\"ERROR\",\"message\":\"error log multiple parameter arg1 arg2\"}",
          jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Error",
          "{\"level\":\"ERROR\",\"message\":\"error log multiple parameter arg1 arg2 arg3\"}",
          jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Error",
          "{\"level\":\"ERROR\",\"message\":\"error log multiple parameter arg1 arg2 arg3 arg4\"}",
          jsonLine);
      jsonLine = reader.readLine();
      Assert.assertTrue(
          "Verify logLevel-Info with Exception",
          jsonLine.contains(
              "{\"level\":\"INFO\",\"message\":\"test log\",\"stack_trace\":\"java.lang.RuntimeException: error"));
      jsonLine = reader.readLine();
      Assert.assertTrue(
          "Verify logLevel-Debug with Exception",
          jsonLine.contains(
              "{\"level\":\"DEBUG\",\"message\":\"debug log parameterized\",\"stack_trace\":\"java.lang.RuntimeException: error"));
      jsonLine = reader.readLine();
      Assert.assertTrue(
          "Verify logLevel-Warn with Exception",
          jsonLine.contains(
              "{\"level\":\"WARN\",\"message\":\"warn log multiple parameter arg1 arg2 arg3\",\"stack_trace\":\"java.lang.RuntimeException: error"));
      jsonLine = reader.readLine();
      Assert.assertTrue(
          "Verify logLevel-Error with Exception",
          jsonLine.contains(
              "{\"level\":\"ERROR\",\"message\":\"error log multiple parameter arg1 arg2 arg3 arg4\",\"stack_trace\":\"java.lang.RuntimeException: error"));
    }
  }

  @Test
  public void testStructuredLoggingAllProviders() throws IOException {
    File logFile =
        addFileAppenderToLogger(
            localLoggerContext.getLogger(LOGGER_NAME),
            new LogLevelJsonProvider(),
            new MessageJsonProvider(),
            new StackTraceJsonProvider(),
            new ArgumentsJsonProvider());

    ProtobufStructuredLogger<StructuredLogData> structuredLogger =
        new ProtobufStructuredLogger<>(localLoggerContext.getLogger(LOGGER_NAME));

    long startTime = System.currentTimeMillis();
    long endTime = System.currentTimeMillis() + 1000;
    StructuredLogData.Builder builder = StructuredLogData.newBuilder();
    builder.setId("id-1");
    builder.setMessage("Log Data message");
    builder.setStart(startTime);
    builder.setFinish(endTime);
    builder.setOptionalMessage("Optional Message");
    StructuredLogData logStruct = builder.build();

    structuredLogger.info(logStruct, "test log");
    structuredLogger.debug(logStruct, "debug log {}", "parameterized");
    structuredLogger.warn(logStruct, "warn log multiple parameter {} {}", "arg1", "arg2");
    structuredLogger.error(
        logStruct, "error log multiple parameter {} {} {} {}", "arg1", "arg2", "arg3", "arg4");

    structuredLogger.info(logStruct, "test log", new RuntimeException("error"));
    structuredLogger.debug(
        logStruct, "debug log {}", "parameterized", new RuntimeException("error"));
    structuredLogger.warn(
        logStruct,
        "warn log multiple parameter {} {} {}",
        "arg1",
        "arg2",
        "arg3",
        new RuntimeException("error"));
    structuredLogger.error(
        logStruct,
        "error log multiple parameter {} {} {} {}",
        "arg1",
        "arg2",
        "arg3",
        "arg4",
        new RuntimeException("error"));

    try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
      String jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Info",
          "{\"level\":\"INFO\",\"message\":\"test log\",\"id\":\"id-1\",\"message\":\"Log Data message\",\"start\":"
              + startTime
              + ",\"finish\":"
              + endTime
              + ",\"optionalMessage\":\"Optional Message\"}",
          jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Debug",
          "{\"level\":\"DEBUG\",\"message\":\"debug log parameterized\",\"id\":\"id-1\",\"message\":\"Log Data message\",\"start\":"
              + startTime
              + ",\"finish\":"
              + endTime
              + ",\"optionalMessage\":\"Optional Message\"}",
          jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Warn",
          "{\"level\":\"WARN\",\"message\":\"warn log multiple parameter arg1 arg2\",\"id\":\"id-1\",\"message\":\"Log Data message\",\"start\":"
              + startTime
              + ",\"finish\":"
              + endTime
              + ",\"optionalMessage\":\"Optional Message\"}",
          jsonLine);
      jsonLine = reader.readLine();
      Assert.assertEquals(
          "Verify logLevel-Error",
          "{\"level\":\"ERROR\",\"message\":\"error log multiple parameter arg1 arg2 arg3 arg4\",\"id\":\"id-1\",\"message\":\"Log Data message\",\"start\":"
              + startTime
              + ",\"finish\":"
              + endTime
              + ",\"optionalMessage\":\"Optional Message\"}",
          jsonLine);
      jsonLine = reader.readLine();
      Assert.assertTrue(
          "Verify logLevel-Info with Exception",
          jsonLine.contains(
              "{\"level\":\"INFO\",\"message\":\"test log\",\"stack_trace\":\"java.lang.RuntimeException: error"));
      Assert.assertTrue(
          "Verify logLevel-Info with Exception args",
          jsonLine.contains(
              "id\":\"id-1\",\"message\":\"Log Data message\",\"start\":"
                  + startTime
                  + ",\"finish\":"
                  + endTime
                  + ",\"optionalMessage\":\"Optional Message\""));
      jsonLine = reader.readLine();
      Assert.assertTrue(
          "Verify logLevel-Debug with Exception",
          jsonLine.contains(
              "{\"level\":\"DEBUG\",\"message\":\"debug log parameterized\",\"stack_trace\":\"java.lang.RuntimeException: error"));
      Assert.assertTrue(
          "Verify logLevel-Debug with Exception args",
          jsonLine.contains(
              "id\":\"id-1\",\"message\":\"Log Data message\",\"start\":"
                  + startTime
                  + ",\"finish\":"
                  + endTime
                  + ",\"optionalMessage\":\"Optional Message\""));
      jsonLine = reader.readLine();
      Assert.assertTrue(
          "Verify logLevel-Warn with Exception",
          jsonLine.contains(
              "{\"level\":\"WARN\",\"message\":\"warn log multiple parameter arg1 arg2 arg3\",\"stack_trace\":\"java.lang.RuntimeException: error"));
      Assert.assertTrue(
          "Verify logLevel-Warn with Exception args",
          jsonLine.contains(
              "id\":\"id-1\",\"message\":\"Log Data message\",\"start\":"
                  + startTime
                  + ",\"finish\":"
                  + endTime
                  + ",\"optionalMessage\":\"Optional Message\""));
      jsonLine = reader.readLine();
      Assert.assertTrue(
          "Verify logLevel-Error with Exception",
          jsonLine.contains(
              "{\"level\":\"ERROR\",\"message\":\"error log multiple parameter arg1 arg2 arg3 arg4\",\"stack_trace\":\"java.lang.RuntimeException: error"));
      Assert.assertTrue(
          "Verify logLevel-Error with Exception args",
          jsonLine.contains(
              "id\":\"id-1\",\"message\":\"Log Data message\",\"start\":"
                  + startTime
                  + ",\"finish\":"
                  + endTime
                  + ",\"optionalMessage\":\"Optional Message\""));
    }
  }

  @Test
  public void testStructuredLoggingOff() throws IOException {
    logger.setLevel(Level.OFF);
    File logFile =
        addFileAppenderToLogger(
            localLoggerContext.getLogger(LOGGER_NAME),
            new LogLevelJsonProvider(),
            new MessageJsonProvider());

    ProtobufStructuredLogger<StructuredLogData> structuredLogger =
        new ProtobufStructuredLogger<>(localLoggerContext.getLogger(LOGGER_NAME));

    StructuredLogData.Builder builder = StructuredLogData.newBuilder();
    builder.setId("id-1");
    builder.setMessage("Log Data message");
    builder.setStart(System.currentTimeMillis());
    builder.setFinish(System.currentTimeMillis() + 1000);
    builder.setOptionalMessage("Optional Message");
    StructuredLogData logStruct = builder.build();

    structuredLogger.info(logStruct, "test log");

    try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
      String jsonLine = reader.readLine();
      Assert.assertEquals(null, jsonLine);
    }

    logger.setLevel(Level.ALL);
  }

  @Test
  public void testConstructArgsArray() {
    StructuredLogData.Builder builder = StructuredLogData.newBuilder();
    builder.setId("id-1");
    builder.setMessage("Log Data message");
    builder.setStart(System.currentTimeMillis());
    builder.setFinish(System.currentTimeMillis() + 1000);
    builder.setOptionalMessage("Optional Message");
    StructuredLogData logStruct = builder.build();

    ProtobufStructuredLogger<StructuredLogData> protobufStructuredLogger =
        new ProtobufStructuredLogger<>(logger);

    Object[] args = protobufStructuredLogger.constructArgsArray(logStruct, "arg1", 2, "arg3");
    Assert.assertEquals(args[0], "arg1");
    Assert.assertEquals(args[1], 2);
    Assert.assertEquals(args[2], "arg3");
    Assert.assertEquals(args[3], StructuredArguments.f(logStruct));

    RuntimeException exception = new RuntimeException("error");

    args = protobufStructuredLogger.constructArgsArray(logStruct, "arg1", 2, "arg3", exception);
    Assert.assertEquals(args[0], "arg1");
    Assert.assertEquals(args[1], 2);
    Assert.assertEquals(args[2], "arg3");
    Assert.assertEquals(args[3], StructuredArguments.f(logStruct));
    Assert.assertEquals(args[4], exception);
  }
}
