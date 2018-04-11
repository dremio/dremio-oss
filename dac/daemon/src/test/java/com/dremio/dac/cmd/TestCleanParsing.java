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
package com.dremio.dac.cmd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.beust.jcommander.ParameterException;

/**
 * Tests Clean command parsing. Mostly pointless.
 */
public class TestCleanParsing {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void parseCompact() {
    assertFalse(Clean.Options.parse(new String[] {}).isCompactKvStore());
    assertTrue(Clean.Options.parse(new String[] {"-c"}).isCompactKvStore());
    assertTrue(Clean.Options.parse(new String[] {"--compact"}).isCompactKvStore());
  }

  @Test
  public void parseReindex() {
    assertFalse(Clean.Options.parse(new String[] {}).isReindexData());
    assertTrue(Clean.Options.parse(new String[] {"-i"}).isReindexData());
    assertTrue(Clean.Options.parse(new String[] {"--reindex-data"}).isReindexData());
  }

  @Test
  public void parseOrphans() {
    assertFalse(Clean.Options.parse(new String[] {}).isDeleteOrphans());
    assertTrue(Clean.Options.parse(new String[] {"-o"}).isDeleteOrphans());
    assertTrue(Clean.Options.parse(new String[] {"--delete-orphans"}).isDeleteOrphans());
  }

  @Test
  public void parseMaxJobs() {
    assertEquals(Integer.MAX_VALUE, Clean.Options.parse(new String[] {}).getMaxJobDays());
    assertEquals(30, Clean.Options.parse(new String[] {"-j", "30"}).getMaxJobDays());
    assertEquals(30, Clean.Options.parse(new String[] {"--max-job-days", "30"}).getMaxJobDays());
  }

  @Test
  public void parseWrongJobs() {
    thrown.expect(ParameterException.class);
    thrown.expectMessage("couldn't convert \"abc\" to an integer");
    assertEquals(30, Clean.Options.parse(new String[] {"-j", "abc"}).getMaxJobDays());
  }

  @Test
  public void reportInvalidParameter() {
    thrown.expect(ParameterException.class);
    thrown.expectMessage("no main parameter was defined");
    Clean.Options.parse(new String[] {"--my-fake-parameter"});
  }
}
