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
package com.dremio.TestBlockLevel;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.dremio.common.logging.obfuscation.TestBlockLevel.A.First.AFirst;
import com.dremio.common.logging.obfuscation.TestBlockLevel.A.Second.ASecond;
import com.dremio.common.logging.obfuscation.TestBlockLevel.A.Third.AThird;
import com.dremio.common.logging.obfuscation.TestBlockLevel.B.First.BFirst;
import com.dremio.common.logging.obfuscation.TestBlockLevel.B.Second.BSecond;
import com.dremio.common.logging.obfuscation.TestBlockLevel.B.Third.BThird;
import com.dremio.common.logging.obfuscation.TestBlockLevel.C.CFirst;
import com.dremio.common.logging.obfuscation.TestBlockLevel.C.Second.CSecond;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/** expectations in this test depend on the log configuration in the logback-test.xml resource */
public class TestBlockLevelLogging {

  private static void assertLowestLogLevel(List<ILoggingEvent> logsList, Level level) {
    Level lowestLevel = null;
    for (ILoggingEvent event : logsList) {
      Level eventLevel = event.getLevel();
      if (lowestLevel == null || lowestLevel.isGreaterOrEqual(eventLevel)) {
        lowestLevel = eventLevel;
      }
    }
    Assert.assertNotNull(lowestLevel);
    Assert.assertTrue(lowestLevel.isGreaterOrEqual(level));
  }

  public static List<ILoggingEvent> testLogFilteringUtil(org.slf4j.Logger slf4jLogger) {
    ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) slf4jLogger;
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

  @Test
  public void testAFirst() {
    AFirst aFirst = new AFirst();
    List<ILoggingEvent> logsList = aFirst.testLogFiltering();
    assertLowestLogLevel(logsList, Level.ERROR);
  }

  @Test
  public void testASecond() {
    ASecond aSecond = new ASecond();
    List<ILoggingEvent> logsList = aSecond.testLogFiltering();
    assertLowestLogLevel(logsList, Level.WARN);
  }

  @Test
  public void testAThird() {
    AThird aThird = new AThird();
    List<ILoggingEvent> logsList = aThird.testLogFiltering();
    assertLowestLogLevel(logsList, Level.WARN);
  }

  @Test
  public void testBFirst() {
    BFirst bFirst = new BFirst();
    List<ILoggingEvent> logsList = bFirst.testLogFiltering();
    assertLowestLogLevel(logsList, Level.INFO);
  }

  @Test
  public void testBSecond() {
    BSecond bSecond = new BSecond();
    List<ILoggingEvent> logsList = bSecond.testLogFiltering();
    assertLowestLogLevel(logsList, Level.ERROR);
  }

  @Test
  public void testBThird() {
    BThird bThird = new BThird();
    List<ILoggingEvent> logsList = bThird.testLogFiltering();
    assertLowestLogLevel(logsList, Level.DEBUG);
  }

  @Test
  public void testCFirst() {
    CFirst cFirst = new CFirst();
    List<ILoggingEvent> logsList = cFirst.testLogFiltering();
    assertLowestLogLevel(logsList, Level.DEBUG);
  }

  @Test
  public void testCSecond() {
    CSecond cSecond = new CSecond();
    List<ILoggingEvent> logsList = cSecond.testLogFiltering();
    assertLowestLogLevel(logsList, Level.TRACE);
  }
}
