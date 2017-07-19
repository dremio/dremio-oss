/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.compile;

import java.io.IOException;
import java.util.Arrays;

import org.codehaus.commons.compiler.CompileException;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.compile.ClassTransformer.ClassNames;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.server.options.OptionValidator;
import com.dremio.exec.server.options.OptionValue;
import com.dremio.exec.server.options.Options;
import com.dremio.exec.server.options.TypeValidators.BooleanValidator;
import com.dremio.exec.server.options.TypeValidators.LongValidator;
import com.dremio.exec.server.options.TypeValidators.StringValidator;

/**
 * {@code ClassCompiler} factory choosing the right instance
 * based on config and session options.
 */
@Options
public class ClassCompilerSelector {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClassCompilerSelector.class);

  public enum CompilerPolicy {
    DEFAULT, JDK, JANINO;
  }

  public static final String JAVA_COMPILER_OPTION = "exec.java_compiler";
  public static final StringValidator JAVA_COMPILER_VALIDATOR = new StringValidator(JAVA_COMPILER_OPTION, CompilerPolicy.DEFAULT.toString()) {
    @Override
    public void validate(OptionValue v) {
      super.validate(v);
      try {
        CompilerPolicy.valueOf(v.string_val.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw UserException.validationError()
            .message("Invalid value '%s' specified for option '%s'. Valid values are %s.",
              v.string_val, getOptionName(), Arrays.toString(CompilerPolicy.values()))
            .build(logger);
      }
    }
  };

  public static final String JAVA_COMPILER_DEBUG_OPTION = "exec.java_compiler_debug";
  public static final OptionValidator JAVA_COMPILER_DEBUG = new BooleanValidator(JAVA_COMPILER_DEBUG_OPTION, true);

  public static final String JAVA_COMPILER_JANINO_MAXSIZE_OPTION = "exec.java_compiler_janino_maxsize";
  public static final OptionValidator JAVA_COMPILER_JANINO_MAXSIZE = new LongValidator(JAVA_COMPILER_JANINO_MAXSIZE_OPTION, 256*1024);

  public static final String JAVA_COMPILER_CONFIG = "dremio.exec.compile.compiler";
  public static final String JAVA_COMPILER_DEBUG_CONFIG = "dremio.exec.compile.debug";
  public static final String JAVA_COMPILER_JANINO_MAXSIZE_CONFIG = "dremio.exec.compile.janino_maxsize";

  private final CompilerPolicy defaultPolicy;
  private final long defaultJaninoThreshold;
  private final boolean defaultDebug;

  private final ClassCompiler jdkClassCompiler;
  private final ClassCompiler janinoClassCompiler;

  private final OptionManager sessionOptions;


  public ClassCompilerSelector(SabotConfig config, OptionManager sessionOptions) {
    this.sessionOptions = sessionOptions;

    this.defaultPolicy = CompilerPolicy.valueOf(config.getString(JAVA_COMPILER_CONFIG).toUpperCase());
    this.defaultJaninoThreshold = config.getLong(JAVA_COMPILER_JANINO_MAXSIZE_CONFIG);
    this.defaultDebug = config.getBoolean(JAVA_COMPILER_DEBUG_CONFIG);

    this.janinoClassCompiler = new JaninoClassCompiler();
    this.jdkClassCompiler = JDKClassCompiler.newInstance();
  }

  public ClassBytes[] getClassByteCode(ClassNames className, String sourceCode)
      throws CompileException, ClassNotFoundException, ClassTransformationException, IOException {
    OptionValue value = sessionOptions.getOption(JAVA_COMPILER_OPTION);
    CompilerPolicy policy = (value != null) ? CompilerPolicy.valueOf(value.string_val.toUpperCase()) : defaultPolicy;

    value = sessionOptions.getOption(JAVA_COMPILER_JANINO_MAXSIZE_OPTION);
    long janinoThreshold = (value != null) ? value.num_val : defaultJaninoThreshold;

    value = sessionOptions.getOption(JAVA_COMPILER_DEBUG_OPTION);
    boolean debug = (value != null) ? value.bool_val : defaultDebug;

    ClassCompiler classCompiler;
    if (jdkClassCompiler != null &&
        (policy == CompilerPolicy.JDK || (policy == CompilerPolicy.DEFAULT && sourceCode.length() > janinoThreshold))) {
      classCompiler = jdkClassCompiler;
    } else {
      classCompiler = janinoClassCompiler;
    }

    ClassBytes[] bc = classCompiler.getClassByteCode(className, sourceCode, debug);
    /*
     * final String baseDir = System.getProperty("java.io.tmpdir") + File.separator + classCompiler.getClass().getSimpleName();
     * File classFile = new File(baseDir + className.clazz);
     * classFile.getParentFile().mkdirs();
     * BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(classFile));
     * out.write(bc[0]);
     * out.close();
     */
    return bc;
  }
}
