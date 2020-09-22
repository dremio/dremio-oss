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
package com.dremio.provision.yarn;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;

import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application bundle runnable
 *
 * Requires a jar generated using {@code AppBundleGenerator}
 */
public class AppBundleRunnable implements TwillRunnable {

  private static final Logger logger = LoggerFactory.getLogger(AppBundleRunnable.class);

  /**
   * Contains runtime arguments for {@code DremioTwillRunner}.
   */
  static class Arguments {

    /**
     * Filename of the bundled jar, as specified in the TwillSpecification local files.
     */
    private final String jarFileName;

    /**
     * Class name of the class having the main() that is to be called.
     */
    private final String mainClassName;

    /**
     * Arguments to pass the main() of the class specified by mainClassName.
     */
    private final String[] mainArgs;

    Arguments(String jarFileName, String mainClassName, String[] mainArgs) {
      this.jarFileName = jarFileName;
      this.mainClassName = mainClassName;
      this.mainArgs = mainArgs;
    }

    public static Arguments fromArray(String[] args) {
      checkArgument(args.length >= 1, "Requires at least 2 argument:"
        + " <jarFileName> <mainClassName>");

      return new Arguments(args[0], args[1], Arrays.copyOfRange(args, 2, args.length));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Arguments arguments = (Arguments) o;
      return Objects.equals(jarFileName, arguments.jarFileName)
          && Objects.equals(mainClassName, arguments.mainClassName)
          && Arrays.equals(mainArgs, arguments.mainArgs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(jarFileName, mainClassName, Arrays.hashCode(mainArgs));
    }

    public String[] toArray() {
      String[] result = new String[2 + mainArgs.length];
      result[0] = jarFileName;
      result[1] = mainClassName;
      for (int i = 0; i < mainArgs.length; i++) {
        result[2 + i] = mainArgs[i];
      }

      return result;
    }

    public String getJarFileName() {
      return jarFileName;
    }

    public String getMainClassName() {
      return mainClassName;
    }

    public String[] getMainArgs() {
      return mainArgs;
    }
  }

  /**
   * Runs the bundled jar.
   */
  private AppBundleRunner jarRunner;

  /**
   * Arguments for running the bundled jar.
   */
  private Arguments arguments;

  @Override
  public void stop() {
    if (jarRunner != null) {
      try {
        jarRunner.stop();
      } catch (Throwable t) {
        logger.error("Error occurred upon stopping jarRunner: ", t);
      }
    }
  }

  @Override
  public void run() {
    Objects.requireNonNull(jarRunner);

    try {
      jarRunner.run();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public TwillRunnableSpecification configure() {
    return TwillRunnableSpecification.Builder.with()
      .setName(getName())
      .noConfigs()
      .build();
  }

  private AppBundleRunner loadJarRunner(File jarFile, Arguments arguments) {
    AppBundleRunner jarRunner = new AppBundleRunner(jarFile, arguments);

    try {
      jarRunner.load();
      return jarRunner;
    } catch (Exception e) {
      logger.error("Error loading classes into jarRunner", e);
    }

    return null;
  }

  @Override
  public final void initialize(TwillContext context) {
    arguments = Arguments.fromArray(context.getArguments());

    final File jarFile = new File(arguments.getJarFileName());
    Objects.requireNonNull(jarFile, String.format("Jar file %s cannot be null", jarFile.getAbsolutePath()));
    checkArgument(jarFile.exists(), "Jar file %s must exist", jarFile.getAbsolutePath());
    checkArgument(jarFile.canRead(), "Jar file %s must be readable", jarFile.getAbsolutePath());

    jarRunner = loadJarRunner(jarFile, arguments);
  }

  @Override
  public void handleCommand(org.apache.twill.api.Command command) throws Exception {
    // No-op by default. Left for children class to override.
  }

  @Override
  public void destroy() {
    if (jarRunner != null) {
      try {
        jarRunner.close();
      } catch (Exception ex) {
        logger.error("Error occurred upon close of jarRunner: ", ex);
      }
    }
  }

  // Implement own checkArgument to avoid dependency on Guava Preconditions
  private static void checkArgument(boolean b, String errorMessageTemplate) {
    if (!b) {
      throw new IllegalArgumentException(errorMessageTemplate);
    }
  }

  // Implement own checkArgument to avoid dependency on Guava Preconditions
  private static void checkArgument(boolean b, String errorMessageTemplate, Object p1) {
    if (!b) {
      throw new IllegalArgumentException(String.format(errorMessageTemplate, p1));
    }
  }
}
