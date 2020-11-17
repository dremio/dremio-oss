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

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.dac.server.DACConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Runs an admin command.
 */
public final class AdminCommandRunner {

  public static void main(String[] args) throws Exception {
    final SabotConfig sabotConfig = DACConfig.newConfig().getConfig().getSabotConfig();
    final ScanResult classpathScan = ClassPathScanner.fromPrescan(sabotConfig);

    final List<Class<?>> adminCommands = classpathScan.getAnnotatedClasses(AdminCommand.class);
    if (args.length < 1) {
      printUsage("Missing action argument", adminCommands, System.err);
      System.exit(1);
    }

    final String commandName = args[0];

    if ("-h".equals(commandName) || "--help".equals(commandName)) {
      printUsage("", adminCommands, System.out);
      System.exit(0);
    }

    Class<?> command = null;
    for (Class<?> clazz : adminCommands) {
      final AdminCommand adminCommand = clazz.getAnnotation(AdminCommand.class);
      if (adminCommand.value().equals(commandName)) {
        command = clazz;
        break;
      }
    }

    if (command == null) {
      printUsage(String.format("Unknown action: '%s'", commandName), adminCommands, System.err);
      System.exit(2);
    }

    try {
      runCommand(commandName, command, Arrays.copyOfRange(args, 1, args.length));
    } catch (Exception e) {
      AdminLogger.log(String.format("Failed to run '%s' command: %s", commandName, e.getMessage()));
      throw e;
    }
  }

  static void runCommand(String commandName, Class<?> command, String[] commandArgs) throws Exception {
    final Method mainMethod = command.getMethod("main", String[].class);
    Preconditions.checkState(Modifier.isStatic(mainMethod.getModifiers())
        && Modifier.isPublic(mainMethod.getModifiers()), "#main(String[]) must have public and static modifiers");

    final Object[] objects = new Object[1];
    objects[0] = commandArgs;
    try {
      //noinspection JavaReflectionInvocation
      mainMethod.invoke(null, objects);
    } catch (final ReflectiveOperationException e) {
      final Throwable cause = e.getCause() != null ? e.getCause() : e;
      Throwables.throwIfUnchecked(cause);
      throw (Exception)cause;
    }
  }

  private static void printUsage(String reason, List<Class<?>> adminCommands, PrintStream stream) {
    if (!reason.isEmpty()) {
      stream.println(reason);
    }

    final String commandNames = adminCommands.stream()
        .map(aClass -> aClass.getAnnotation(AdminCommand.class))
        .map(AdminCommand::value)
        .collect(Collectors.joining("|"));

    stream.println("Usage: dremio-admin (" + commandNames + ") [args...]");

    adminCommands.forEach(clazz -> {
      final AdminCommand adminCommand = clazz.getAnnotation(AdminCommand.class);
      stream.println("  " + adminCommand.value() + ": " + adminCommand.description());
    });

    stream.println("Run export 'DREMIO_ADMIN_LOG_DIR=<path>' to set log directory.");
    stream.println("Run export 'DREMIO_ADMIN_LOG_VERBOSITY=<value>' to set verbosity. Default is INFO.");
  }
}
