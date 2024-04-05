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

import com.dremio.service.functions.model.Function;
import com.dremio.service.functions.model.FunctionCategory;
import com.dremio.service.functions.model.FunctionSignature;
import com.dremio.service.functions.model.Parameter;
import com.dremio.service.functions.model.SampleCode;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test to assert that all the function specs are in a valid format.
 *
 * <p>Things like "no duplicate specs" and no "duplicate categories" are validated here.
 */
public final class FunctionListFormatValidationTest {
  @Test
  public void testNoDuplicateTypeSignatures() {
    assertPerFunction(
        new FunctionVisitor() {
          @Override
          protected void onFunctionSignatures(List<FunctionSignature> signatures) {
            Set<FunctionSignature> functionSignatureAsSet = new HashSet<>();
            for (FunctionSignature functionSignature : signatures) {
              List<Parameter> parametersWithOnlyTypeInformation =
                  functionSignature.getParameters().stream()
                      .map(
                          parameter ->
                              Parameter.builder()
                                  .kind(parameter.getKind())
                                  .type(parameter.getType())
                                  .build())
                      .collect(Collectors.toList());
              FunctionSignature signatureWithOnlyTypeInformation =
                  FunctionSignature.builder()
                      .returnType(functionSignature.getReturnType())
                      .addAllParameters(parametersWithOnlyTypeInformation)
                      .build();

              Assert.assertTrue(
                  "Failed to add the following type signature '" + functionSignature + "'",
                  functionSignatureAsSet.add(signatureWithOnlyTypeInformation));
            }
          }
        });
  }

  @Test
  public void testNoDuplicateFunctionSignatures() {
    assertPerFunction(
        new FunctionVisitor() {
          @Override
          protected void onFunctionSignatures(List<FunctionSignature> signatures) {
            Set<FunctionSignature> functionSignatureAsSet = new HashSet<>();
            for (FunctionSignature functionSignature : signatures) {
              Assert.assertTrue(
                  "Failed to add the following signature '" + functionSignature.toString() + "'",
                  functionSignatureAsSet.add(functionSignature));
            }
          }
        });
  }

  @Test
  public void testNoDuplicateFunctionCategory() {
    assertPerFunction(
        new FunctionVisitor() {
          @Override
          protected void onFunctionCategories(List<FunctionCategory> functionCategories) {
            Set<FunctionCategory> functionCategoriesAsSet = new HashSet<>();
            for (FunctionCategory functionCategory : functionCategories) {
              Assert.assertTrue(
                  "Failed to add the following function category '"
                      + functionCategory.toString()
                      + "'",
                  functionCategoriesAsSet.add(functionCategory));
            }
          }
        });
  }

  @Test
  public void testNoDuplicateSamples() {
    assertPerFunction(
        new FunctionVisitor() {
          @Override
          protected void onSampleCodes(List<SampleCode> sampleCodes) {
            Set<SampleCode> sampleCodesAsSet = new HashSet<>();
            for (SampleCode sampleCode : sampleCodes) {
              Assert.assertTrue(
                  "Failed to add the following sample code '" + sampleCode.toString() + "'",
                  sampleCodesAsSet.add(sampleCode));
            }
          }
        });
  }

  @Test
  public void testNoStubsInDocumentedFunctions() {
    assertPerDocumentedFunction(
        new FunctionVisitor() {
          @Override
          protected void onFunctionDescription(String description) {
            Assert.assertFalse(description.startsWith("<"));
            Assert.assertFalse(description.endsWith("<"));
          }

          @Override
          protected void onFunctionSignatureDescription(String description) {
            Assert.assertFalse(description.startsWith("<"));
            Assert.assertFalse(description.endsWith("<"));
          }

          @Override
          protected void onSampleCodeCall(String call) {
            Assert.assertFalse(call.startsWith("<"));
            Assert.assertFalse(call.endsWith("<"));
          }

          @Override
          protected void onSampleCodeResult(String result) {
            Assert.assertFalse(result.startsWith("<"));
            Assert.assertFalse(result.endsWith("<"));
          }
        });
  }

  private static void assertPerFunction(FunctionVisitor functionVisitor) {
    assertPerFunctionImpl(FunctionListDictionary.getFunctionNames(), functionVisitor);
  }

  private static void assertPerDocumentedFunction(FunctionVisitor visitor) {
    assertPerFunctionImpl(FunctionListDictionary.getDocumentedFunctionNames(), visitor);
  }

  private static void assertPerFunctionImpl(Set<String> functionNames, FunctionVisitor visitor) {
    for (String functionName : functionNames) {
      Function function = FunctionListDictionary.tryGetFunction(functionName).get();
      try {
        visitor.visitFunction(function);
      } catch (Exception | Error ex) {
        throw new RuntimeException(
            "Ran into exception when processing function '" + function.getName() + "'", ex);
      }
    }
  }
}
