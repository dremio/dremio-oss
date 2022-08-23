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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.logging.obfuscation.TestBlockLevel.A.First.AFirst;
import com.dremio.common.logging.obfuscation.TestBlockLevel.A.Second.ASecond;
import com.dremio.common.logging.obfuscation.TestBlockLevel.A.Third.AThird;
import com.dremio.common.logging.obfuscation.TestBlockLevel.B.First.BFirst;
import com.dremio.common.logging.obfuscation.TestBlockLevel.B.Second.BSecond;
import com.dremio.common.logging.obfuscation.TestBlockLevel.B.Third.BThird;
import com.dremio.common.logging.obfuscation.TestBlockLevel.C.CFirst;
import com.dremio.common.logging.obfuscation.TestBlockLevel.C.Second.CSecond;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

public class TestBlockLevelLogging {
  boolean isLowestLevelPresentInLogList(List<ILoggingEvent> logsList, Level level)
  {
    for (int i=0;i<logsList.size();i++) {
          if(logsList.get(i).getLevel() == level)
          {
            return true;
          }
    }
    return false;
  }
  public static List<ILoggingEvent> testLogFilteringUtil(ch.qos.logback.classic.Logger logger) {
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
    Assert.assertTrue(isLowestLevelPresentInLogList(logsList,Level.ERROR));
    for (int i=0;i<logsList.size();i++) {
       Assert.assertTrue(logsList.get(i).getLevel().isGreaterOrEqual(Level.toLevel("ERROR")));

    }
  }

  @Test
  public void testASecond() {
    ASecond aSecond = new ASecond();
    List<ILoggingEvent> logsList = aSecond.testLogFiltering();
    Assert.assertTrue(isLowestLevelPresentInLogList(logsList,Level.WARN));
    for (int i=0;i<logsList.size();i++) {
      Assert.assertTrue(logsList.get(i).getLevel().isGreaterOrEqual(Level.toLevel("WARN")));
    }
  }

  @Test
  public void testAThird() {
    AThird aThird = new AThird();
    List<ILoggingEvent> logsList = aThird.testLogFiltering();
    Assert.assertTrue(isLowestLevelPresentInLogList(logsList,Level.WARN));
    for (int i=0;i<logsList.size();i++) {
      Assert.assertTrue(logsList.get(i).getLevel().isGreaterOrEqual(Level.toLevel("WARN")));
    }
  }

  @Test
  public void testBFirst() {
    BFirst bFirst = new BFirst();
    List<ILoggingEvent> logsList = bFirst.testLogFiltering();
    Assert.assertTrue(isLowestLevelPresentInLogList(logsList,Level.INFO));
    for (int i=0;i<logsList.size();i++) {
      Assert.assertTrue(logsList.get(i).getLevel().isGreaterOrEqual(Level.toLevel("INFO")));
    }
  }

  @Test
  public void testBSecond() {
    BSecond bSecond = new BSecond();
    List<ILoggingEvent> logsList = bSecond.testLogFiltering();
    Assert.assertTrue(isLowestLevelPresentInLogList(logsList,Level.ERROR));
    for (int i=0;i<logsList.size();i++) {
      Assert.assertTrue(logsList.get(i).getLevel().isGreaterOrEqual(Level.toLevel("ERROR")));
    }
  }

  @Test
  public void testBThird() {
    BThird bThird = new BThird();
    List<ILoggingEvent> logsList = bThird.testLogFiltering();
    Assert.assertTrue(isLowestLevelPresentInLogList(logsList,Level.DEBUG));
    for (int i=0;i<logsList.size();i++) {
      Assert.assertTrue(logsList.get(i).getLevel().isGreaterOrEqual(Level.toLevel("DEBUG")));
    }
  }

  @Test
  public void testCFirst() {
    CFirst cFirst = new CFirst();
    List<ILoggingEvent> logsList = cFirst.testLogFiltering();
    Assert.assertTrue(isLowestLevelPresentInLogList(logsList,Level.DEBUG));
    for (int i=0;i<logsList.size();i++) {
      Assert.assertTrue(logsList.get(i).getLevel().isGreaterOrEqual(Level.toLevel("DEBUG")));
    }
  }
  @Test
  public void testCSecond() {
    CSecond cSecond = new CSecond();
    List<ILoggingEvent> logsList = cSecond.testLogFiltering();
    Assert.assertTrue(isLowestLevelPresentInLogList(logsList,Level.TRACE));
    Assert.assertTrue(Level.toLevel("TRACE").isGreaterOrEqual(Level.toLevel("TRACE")));

    for (int i=0;i<logsList.size();i++) {
      Assert.assertTrue(logsList.get(i).getLevel().isGreaterOrEqual(Level.toLevel("TRACE")));
    }
  }

}
