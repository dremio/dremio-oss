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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for {@code VM}
 */
public class TestVM {

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

  @ParameterizedTest
  @MethodSource("getParameters")
  public void checkMaxDirectMemory(long maxDirectMemory, boolean debugEnabled, List<String> arguments) {
    assertThat(VM.maxDirectMemory(arguments)).isEqualTo(maxDirectMemory);
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void checkDebugEnabled(long maxDirectMemory, boolean debugEnabled, List<String> arguments) {
    assertThat(VM.isDebugEnabled(arguments)).isEqualTo(debugEnabled);
  }

  @Test
  public void checkGetCurrentStackTraceAsString() {
    assertThat(VM.getCurrentStackTraceAsString())
      .startsWith("com.dremio.common.TestVM.checkGetCurrentStackTraceAsString")
      .contains("org.junit");
  }
}
