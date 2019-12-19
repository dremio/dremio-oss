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
package com.dremio.exec.expr;

import org.apache.arrow.gandiva.exceptions.GandivaException;

import com.dremio.common.expression.SupportedEngines;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.fn.GandivaRegistryWrapper;
import com.dremio.options.OptionManager;

public class ExpressionEvaluationOptions {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionEvaluationOptions.class);
  private static boolean javaOnly = false;

  static {
    try {
      GandivaRegistryWrapper.getInstance();
    } catch (GandivaException gandivaException) {
      logger.error("Could not initialize gandiva registry; Using Java code generation.", gandivaException);
      javaOnly = true;
    }
  }

  private final OptionManager options;
  private SupportedEngines.CodeGenOption codeGenOption = SupportedEngines.CodeGenOption.DEFAULT;

  public ExpressionEvaluationOptions(OptionManager options) {
    this.options = options;
  }

  public void setCodeGenOption(String codeGenOption) {
    if (javaOnly) {
      this.codeGenOption = SupportedEngines.CodeGenOption.Java;
    } else {
      // convert the string value to the enum value
      this.codeGenOption = SupportedEngines.CodeGenOption.getCodeGenOption(codeGenOption);
    }
  }

  public SupportedEngines.CodeGenOption getCodeGenOption() {
    return codeGenOption;
  }

  public double getWorkThresholdForSplit() { return options.getOption(ExecConstants.WORK_THRESHOLD_FOR_SPLIT); }

  public boolean isSplitEnabled() { return options.getOption(ExecConstants.SPLIT_ENABLED); }

  public ExpressionEvaluationOptions flipPreferredCodeGen() {
    ExpressionEvaluationOptions clone = new ExpressionEvaluationOptions(options);
    if (this.codeGenOption == SupportedEngines.CodeGenOption.Java) {
      clone.setCodeGenOption(SupportedEngines.CodeGenOption.Gandiva.toString());
    } else {
      clone.setCodeGenOption(SupportedEngines.CodeGenOption.Java.toString());
    }
    return clone;
  }
}
