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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * TRACE < DEBUG. < INFO < WARN < ERROR.
 */
public class TestBlockLevelLogging {
  @Test
  public void testAFirst() {
    AFirst aFirst = new AFirst();
    List<ILoggingEvent> logsList = aFirst.testLogFiltering();
    for (int i=0;i<logsList.size();i++) {
       Assert.assertTrue(logsList.get(i).getLevel().isGreaterOrEqual(Level.toLevel("ERROR")));
    }
  }

  @Test
  public void testASecond() {
    ASecond aSecond = new ASecond();
    List<ILoggingEvent> logsList = aSecond.testLogFiltering();
    for (int i=0;i<logsList.size();i++) {
      Assert.assertTrue(logsList.get(i).getLevel().isGreaterOrEqual(Level.toLevel("ERROR")));
    }
  }

  @Test
  public void testAThird() {
    AThird aThird = new AThird();
    List<ILoggingEvent> logsList = aThird.testLogFiltering();
    for (int i=0;i<logsList.size();i++) {
      Assert.assertTrue(logsList.get(i).getLevel().isGreaterOrEqual(Level.toLevel("WARN")));
    }
  }

  @Test
  public void testBFirst() {
    BFirst bFirst = new BFirst();
    List<ILoggingEvent> logsList = bFirst.testLogFiltering();
    for (int i=0;i<logsList.size();i++) {
      Assert.assertTrue(logsList.get(i).getLevel().isGreaterOrEqual(Level.toLevel("INFO")));
    }
  }

  @Test
  public void testBSecond() {
    BSecond bSecond = new BSecond();
    List<ILoggingEvent> logsList = bSecond.testLogFiltering();
    for (int i=0;i<logsList.size();i++) {
      Assert.assertTrue(logsList.get(i).getLevel().isGreaterOrEqual(Level.toLevel("ERROR")));
    }
  }

  @Test
  public void testBThird() {
    BThird bThird = new BThird();
    List<ILoggingEvent> logsList = bThird.testLogFiltering();
    for (int i=0;i<logsList.size();i++) {
      Assert.assertTrue(logsList.get(i).getLevel().isGreaterOrEqual(Level.toLevel("INFO")));
    }
  }

  @Test
  public void testCFirst() {
    CFirst cFirst = new CFirst();
    List<ILoggingEvent> logsList = cFirst.testLogFiltering();
    for (int i=0;i<logsList.size();i++) {
      Assert.assertTrue(logsList.get(i).getLevel().isGreaterOrEqual(Level.toLevel("TRACE")));
    }
  }

}
