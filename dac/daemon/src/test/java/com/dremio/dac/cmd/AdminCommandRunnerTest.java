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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Test admin command runner.
 */
public class AdminCommandRunnerTest {

  /**
   * Test command.
   */
  private static final class TestCommand {

    private static boolean invokedCorrectly = false;

    public static void main(String[] args) {
      if (args.length == 2 && args[0].equals("arg0") && args[1].equals("arg1")) {
        invokedCorrectly = true;
      }
    }
  }

  @Test
  public void runCommand() throws Exception {
    assertFalse(TestCommand.invokedCorrectly);
    AdminCommandRunner.runCommand("test-command", TestCommand.class, new String[]{"arg0", "arg1"});
    assertTrue(TestCommand.invokedCorrectly);
  }
}
