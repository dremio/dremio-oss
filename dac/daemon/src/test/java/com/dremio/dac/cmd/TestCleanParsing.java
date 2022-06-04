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
package com.dremio.dac.cmd;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

/**
 * Tests Clean command parsing. Mostly pointless.
 */
public class TestCleanParsing {

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
  public void parseWrongJobsAlpha() {
    Clean.Options args = new Clean.Options();
    JCommander jc = JCommander.newBuilder().addObject(args).build();

    assertThatThrownBy(() -> jc.parse("-j", "abc"))
      .isInstanceOf(ParameterException.class)
      .hasMessageContaining("Parameter -j should be a positive integer (found abc)");
  }

  @Test
  public void parseWrongJobsNumeric() {
    Clean.Options args = new Clean.Options();
    JCommander jc = JCommander.newBuilder().addObject(args).build();

    assertThatThrownBy(() -> jc.parse("-j", "-345"))
      .isInstanceOf(ParameterException.class)
      .hasMessageContaining("Parameter -j should be a positive integer (found -345)");
  }

  @Test
  public void reportInvalidParameter() {
    Clean.Options args = new Clean.Options();
    JCommander jc = JCommander.newBuilder().addObject(args).build();

    assertThatThrownBy(() -> jc.parse("--my-fake-parameter"))
      .isInstanceOf(ParameterException.class)
      .hasMessageContaining("no main parameter was defined");
  }
}
