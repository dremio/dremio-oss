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
package com.dremio.common;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Unit tests for {@code VM}
 */
@RunWith(Parameterized.class)
public class TestVM {

  private final List<String> arguments;
  private final long maxDirectMemory;
  private final boolean debugEnabled;

  @Parameters(name = "{index}: {2}")
  public static final Object[][] getParameters() {
    return new Object[][] {
      { 1024L, false, Arrays.asList("-Xmx1g", "-XX:MaxDirectMemorySize=1024") },
      { 2L * 1024, false, Arrays.asList("-Xmx1g", "-XX:MaxDirectMemorySize=2k") },
      { 3L * 1024 * 1024, false, Arrays.asList("-Xmx1g", "-XX:MaxDirectMemorySize=3m") },
      { 4L * 1024 * 1024 * 1024, false, Arrays.asList("-Xmx1g", "-XX:MaxDirectMemorySize=4g") },
      { 5L * 1024 * 1024 * 1024 * 1024, false, Arrays.asList("-Xmx1g", "-XX:MaxDirectMemorySize=5t") },
      { 6L * 1024, false, Arrays.asList("-Xmx1g", "-XX:MaxDirectMemorySize=6K") },
      { 7L * 1024 * 1024, false, Arrays.asList("-Xmx1g", "-XX:MaxDirectMemorySize=7M") },
      { 8L * 1024 * 1024 * 1024, false, Arrays.asList("-Xmx1g", "-XX:MaxDirectMemorySize=8G") },
      { 9L * 1024 * 1024 * 1024 * 1024, false, Arrays.asList("-Xmx1g", "-XX:MaxDirectMemorySize=9T") },
      { 0L , false, Arrays.asList("-Xmx1g", "-XX:MaxDirectMemorySize=8a") },
      { 0L , false, Arrays.asList("-Xmx1g", "-XX:MaxDirectMemorySize=9Gb") },
      { 0L , false, Arrays.asList("-Xmx1g", "-agentlib:jdwp") },
      { 0L , true, Arrays.asList("-Xmx1g", "-Xdebug") },
      { 0L , true, Arrays.asList("-Xmx1g", "-agentlib:jdwp=transport=dt_socket") },
    };
  }

  public TestVM(long maxDirectMemory, boolean debugEnabled, List<String> arguments) {
    this.arguments = arguments;
    this.maxDirectMemory = maxDirectMemory;
    this.debugEnabled = debugEnabled;
  }

  @Test
  public void checkMaxDirectMemory() {
    assertThat(VM.maxDirectMemory(arguments), is(maxDirectMemory));
  }

  @Test
  public void checkDebugEnabled() {
    assertThat(VM.isDebugEnabled(arguments), is(debugEnabled));
  }

}
