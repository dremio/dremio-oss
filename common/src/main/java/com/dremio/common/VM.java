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
package com.dremio.common;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.StandardSystemProperty;

/**
 * Captures important informations about JVM and its current environment
 */
public final class VM {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(VM.class);

  private VM() {
  }

  private static final Pattern MAX_DIRECT_MEMORY_SIZE_ARG_MATCHER =
      Pattern.compile("-XX:MaxDirectMemorySize=(?<amount>\\d+)(?<unit>[kKmMgGtT]?)");

  private static final Pattern DEBUG_ARG_MATCHER =
      Pattern.compile("-Xdebug|-agentlib:jdwp=.*");

  private static final long MAX_DIRECT_MEMORY = maxDirectMemory();

  /**
   * System property key to control the number of cpus used by Dremio
   */
  public static final String DREMIO_CPU_AVAILABLE_PROPERTY = "dremio.cpu.available";

  /**
   * Legacy system property key to control the number of cpus used by Dremio
   */
  private static final String DREMIO_LEGACY_CPU_AVAILABLE_PROPERTY = "dremio.executor.cores";

  private static final boolean IS_DEBUG = isDebugEnabled0();

  /**
   * Returns true if a debugger agent has been added to the JVM
   */
  public static boolean isDebugEnabled() {
    return IS_DEBUG;
  }

  /**
   * Return the number of available processors on the machine
   *
   * Will check first if system property overrides value detected by the
   * virtual machine
   * @return
   */
  public static int availableProcessors() {
    int cpuCount = getIntegerSystemProperty(
        DREMIO_CPU_AVAILABLE_PROPERTY,
        DREMIO_LEGACY_CPU_AVAILABLE_PROPERTY,
        0);
    return cpuCount != 0
        ? cpuCount
        : Runtime.getRuntime().availableProcessors();
  }

  /**
   * Get the maximum amount of directory memory the JVM can allocate, in bytes
   */
  public static long getMaxDirectMemory() {
    return MAX_DIRECT_MEMORY;
  }

  public static long getMaxHeapMemory() {
    return Runtime.getRuntime().maxMemory();
  }

  private static final boolean IS_MACOS_HOST = isMacOSHost0();
  private static final boolean IS_WINDOWS_HOST = isWindowsHost0();

  /**
   * Return if the host is running MacOS operating system
   */
  public static boolean isMacOSHost() {
    return IS_MACOS_HOST;
  }

  /**
   * Return if the host is running Windows operating system
   */
  public static boolean isWindowsHost() {
    return IS_WINDOWS_HOST;
  }

  /* Helper methods for detecting VM settings and Host configuration */
  private static boolean isDebugEnabled0() {
    final List<String> inputArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
    return isDebugEnabled(inputArguments);
  }

  @VisibleForTesting
  static boolean isDebugEnabled(final List<String> inputArguments) {
    for(String argument: inputArguments) {
      if (DEBUG_ARG_MATCHER.matcher(argument).matches()) {
        return true;
      }
    }
    return false;
  }

  private static final long maxDirectMemory() {
    // First try Java8
    try {
      return invokeMaxDirectMemory("sun.misc.VM");
    } catch (ReflectiveOperationException ignored) {
      // ignore
    }

    // Try Java9 or higher
    // Only works if jdk.internal is added to the unnamed module
    try {
      return invokeMaxDirectMemory("jdk.internal.misc.VM");
    } catch (ReflectiveOperationException ignored) {
      // ignore
    }

    // Try mxbeans
    final List<String> inputArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
    long maxDirectMemory = maxDirectMemory(inputArguments);

    if (maxDirectMemory != 0) {
      return maxDirectMemory;
    }

    // default to return max memory
    return Runtime.getRuntime().maxMemory();
  }

  @VisibleForTesting
  static long maxDirectMemory(final List<String> inputArguments) {
    try {
      for(String argument: inputArguments) {
        final Matcher matcher = MAX_DIRECT_MEMORY_SIZE_ARG_MATCHER.matcher(argument);
        if (!matcher.matches()) {
          continue;
        }
        long multiplier = 1;
        switch(matcher.group("unit")) {
        case "t":
        case "T":
          multiplier *= 1024;
        case "g":
        case "G":
          multiplier *= 1024;
        case "m":
        case "M":
          multiplier *= 1024;
        case "k":
        case "K":
          multiplier *= 1024;
        }

        return Long.parseLong(matcher.group("amount")) * multiplier;
      }
    } catch (NumberFormatException e) {
      // Ignore
    }

    return 0;
  }

  private static final long invokeMaxDirectMemory(String className) throws ReflectiveOperationException {
    Class<?> vmClass = Class.forName(className, true, ClassLoader.getSystemClassLoader());
    Method m = vmClass.getDeclaredMethod("maxDirectMemory");
    return ((Number) m.invoke(null)).longValue();
  }

  @VisibleForTesting
  static final boolean isMacOSHost0() {
    String osName = getOSName().replaceAll("[^a-z0-9]+", "");
    return osName.startsWith("macos") || osName.startsWith("osx");
  }

  @VisibleForTesting
  static final boolean isWindowsHost0() {
    String osName = getOSName();
    return osName.contains("win");
  }

  private static final String getOSName() {
    return Optional
        .ofNullable(StandardSystemProperty.OS_NAME.value())
        .orElse("")
        .toLowerCase(Locale.ROOT);
  }

  private static final int getIntegerSystemProperty(String name, String legacyName, int defaultValue) {
    Integer value = Integer.getInteger(name);
    if (value != null) {
      return value;
    }

    value = Integer.getInteger(legacyName);
    if (value != null) {
      LOGGER.warn("System property {} is deprecated. Please use {} instead.", legacyName, name);
      return value;
    }

    return defaultValue;
  }
}
