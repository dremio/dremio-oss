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
package com.dremio.common.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.dremio.common.VM;
import com.google.common.io.Resources;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

public class TestTools {

  private static final TestRule NOP_RULE = (statement, description) -> statement;
  static final String WORKING_PATH = Paths.get("").toAbsolutePath().toString();

  public static TestRule getTimeoutRule() {
    return getTimeoutRule(Duration.ofSeconds(10));
  }

  public static TestRule getTimeoutRule(Duration duration) {
    return getTimeoutRule(duration.getSeconds(), TimeUnit.SECONDS);
  }

  public static TestRule getTimeoutRule(long timeout, TimeUnit unit) {
    if (VM.isDebugEnabled()) {
      // no timeout
      return NOP_RULE;
    }
    return Timeout.builder().withTimeout(timeout, unit).withLookingForStuckThread(true).build();
  }

  /** If not enforced, the repeat rule applies only if the test is run in non-debug mode. */
  public static TestRule getRepeatRule(final boolean enforce) {
    return enforce || !VM.isDebugEnabled() ? new RepeatTestRule() : NOP_RULE;
  }

  public static String getWorkingPath() {
    return WORKING_PATH;
  }

  private static final String PATH_SEPARATOR = System.getProperty("file.separator");
  private static final String[] STRUCTURE = {
    "dremio", "exec", "java-exec", "src", "test", "resources"
  };

  /**
   * Returns fully qualified path where test resources reside if current working directory is at any
   * level in the following root->exec->java-exec->src->test->resources, throws an {@link
   * IllegalStateException} otherwise.
   */
  public static String getTestResourcesPath() {
    final StringBuilder builder = new StringBuilder(WORKING_PATH);
    for (int i = 0; i < STRUCTURE.length; i++) {
      if (WORKING_PATH.endsWith(STRUCTURE[i])) {
        for (int j = i + 1; j < STRUCTURE.length; j++) {
          builder.append(PATH_SEPARATOR).append(STRUCTURE[j]);
        }
        return builder.toString();
      }
    }
    final String msg =
        String.format(
            "Unable to recognize working directory[%s]. The workspace must be root or exec "
                + "module.",
            WORKING_PATH);
    throw new IllegalStateException(msg);
  }

  public static String readTestResourceAsString(String resPath) {
    try {
      if (resPath.startsWith("/")) {
        // for compatibilty with existing callers
        resPath = resPath.substring(1);
      }
      URL resUrl = Resources.getResource(resPath);
      return Resources.toString(resUrl, UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
