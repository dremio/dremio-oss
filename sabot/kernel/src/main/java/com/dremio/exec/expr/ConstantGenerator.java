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

import java.util.HashMap;
import java.util.Map;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.compile.sig.ConstantExtractor;
import com.sun.codemodel.JArray;
import com.sun.codemodel.JAssignmentTarget;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JType;

/**
 * Generates constants defined as class variables either as an array (to reduce the JAVA constant pool usage)
 * or as individual class variables. This is part of the class generator that generates code in JAVA for expressions
 * inside operators.
 * <p>
 *   If the constant extractor has found more than a threshold number of constants this class defines an array
 *   for the different constants in the query (a unique array position is provided for each constant). There will be
 *   a different array for each type of constant and the extractor is used to understand the array size for each type.
 *   If the constant extractor has found less than a threshold number of constants in the expression, this class
 *   generates a unique class variable for each new constant.
 * </p>
 */
public class ConstantGenerator {
  private static final String CONSTANT_PREFIX = "constant";

  // Clazz in which the constant has to be generated.
  private final JDefinedClass clazz;
  // information tracked per constant type, which includes information about next array index to use.
  private final Map<CompleteType, ConstantArrayHolder> holders;

  private ConstantExtractor constantExtractor;
  private int constantArrayThreshold;

  public ConstantGenerator(JDefinedClass clazz) {
    this.constantExtractor = null;
    this.clazz = clazz;
    this.holders = new HashMap<>();
  }

  public void setConstantExtractor(ConstantExtractor constantExtractor, int constantArrayThreshold) {
    this.constantExtractor = constantExtractor;
    this.constantArrayThreshold = constantArrayThreshold;
  }

  public JAssignmentTarget nextConstant(CompleteType type, JType holderType, int uniqueIndex) {
    int constants = 0;
    if (constantExtractor != null && constantExtractor.isLarge(constantArrayThreshold)
      && !constantExtractor.getDiscoveredConstants().isEmpty()) {
      constants = constantExtractor.getDiscoveredConstants().getOrDefault(type, 0);
    }
    if (constants <= 0) {
      return clazz.field(JMod.NONE, holderType, CONSTANT_PREFIX + uniqueIndex);
    }
    final int numConstants = constants;
    final ConstantArrayHolder holder = holders.compute(type,
      (k, v) -> (v == null) ? new ConstantArrayHolder(holderType, numConstants, uniqueIndex) : v);
    return holder.nextConstant();
  }

  private final class ConstantArrayHolder {
    private final int arraySize;
    private final JFieldVar arrayDecl;

    private int currentIndex;

    private ConstantArrayHolder(JType holderType, int numConstants, int uniqueIndex) {
      this.arraySize = numConstants;
      this.currentIndex = 0;
      JArray holders = JExpr.newArray(holderType, this.arraySize);
      arrayDecl = clazz.field(JMod.FINAL, holderType.array(), "constantArray" + uniqueIndex, holders);
    }

    private JAssignmentTarget nextConstant() {
      assert currentIndex < arraySize;
      return arrayDecl.component(JExpr.lit(currentIndex++));
    }
  }
}
