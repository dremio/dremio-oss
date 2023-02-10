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
package com.dremio.service.functions;

import java.util.List;

import com.dremio.service.functions.model.Function;
import com.dremio.service.functions.model.FunctionCategory;
import com.dremio.service.functions.model.FunctionSignature;
import com.dremio.service.functions.model.Parameter;
import com.dremio.service.functions.model.ParameterType;
import com.dremio.service.functions.model.SampleCode;

/**
 * Visitor for the Function Object Model
 */
public abstract class FunctionVisitor {
  public void visitFunction(Function function) {
    onFunction(function);
    onFunctionName(function.getName());
    onFunctionSignatures(function.getSignatures());
    for (FunctionSignature functionSignature : function.getSignatures()) {
      visitFunctionSignature(functionSignature);
    }

    if (function.getDremioVersion() != null) {
      onDremioVersion(function.getDremioVersion());
    }

    if (function.getFunctionCategories() != null) {
      onFunctionCategories(function.getFunctionCategories());
    }

    if (function.getDescription() != null) {
      onFunctionDescription(function.getDescription());
    }
  }

  protected void onFunction(Function function) {
    // Do nothing, but the base class can override
  }

  protected void onFunctionName(String name) {
    // Do nothing, but the base class can override
  }

  protected void onFunctionSignatures(List<FunctionSignature> functionSignatures) {
    // Do nothing, but the base class can override
  }

  private void visitFunctionSignature(FunctionSignature functionSignature) {
    onFunctionSignatureReturnType(functionSignature.getReturnType());
    onFunctionSignatureParameters(functionSignature.getParameters());
    for (Parameter parameter : functionSignature.getParameters()) {
      visitParameter(parameter);
    }

    if (functionSignature.getDescription() != null) {
      onFunctionSignatureDescription(functionSignature.getDescription());
    }

    if (functionSignature.getSampleCodes() != null) {
      onSampleCodes(functionSignature.getSampleCodes());
      for (SampleCode sampleCode : functionSignature.getSampleCodes()) {
        onSampleCode(sampleCode);
        visitSampleCode(sampleCode);
      }
    }
  }

  protected void onFunctionSignature(FunctionSignature functionSignature) {
    // Do nothing, but the base class can override
  }

  protected void onFunctionSignatureReturnType(ParameterType parameterType) {
    // Do nothing, but the base class can override
  }

  protected void onFunctionSignatureParameters(List<Parameter> parameters) {
    // Do nothing, but the base class can override
  }

  protected void onFunctionSignatureDescription(String description) {
    // Do nothing, but the base class can override
  }

  private void visitParameter(Parameter parameter) {
    // TODO
  }

  protected void onSampleCodes(List<SampleCode> sampleCodes) {
    // Do nothing, but the base class can override
  }

  private void visitSampleCode(SampleCode sampleCode) {
    onSampleCodeCall(sampleCode.getCall());
    onSampleCodeResult(sampleCode.getResult());
  }

  protected void onSampleCode(SampleCode sampleCode) {
    // Do nothing, but the base class can override
  }

  protected void onSampleCodeCall(String call) {
    // Do nothing, but the base class can override
  }

  protected void onSampleCodeResult(String result) {
    // Do nothing, but the base class can override
  }

  protected void onDremioVersion(String dremioVersion) {
    // Do nothing, but the base class can override
  }

  protected void onFunctionCategories(List<FunctionCategory> functionCategories) {
    // Do nothing, but the base class can override
  }

  protected void onFunctionDescription(String description) {
    // Do nothing, but the base class can override
  }
}
