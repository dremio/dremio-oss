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
package com.dremio.exec.expr.fn.impl;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.FunctionGenerationHelper;
import javax.inject.Inject;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;

/**
 * Functions for arrays.
 *
 * <p>Note to developers: FieldReader is a UnionListReader that gets passed in from code generation
 * with the curor set to the proper index. This means that something like equals and hashcode get
 * called once per list in the vector.
 */
public class ArrayFunctions {
  @FunctionTemplate(
      names = {"equal", "==", "="},
      scope = FunctionScope.SIMPLE,
      nulls = NullHandling.NULL_IF_NULL)
  public static final class ArrayEquals implements SimpleFunction {
    @Param FieldReader left;
    @Param FieldReader right;
    @Output NullableBitHolder resultHolder;
    @Inject FunctionErrorContext errorContext;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      if (!left.isSet() || !right.isSet()) {
        resultHolder.isSet = 0;
        return;
      }

      resultHolder.isSet = 1;
      boolean areFieldReadersEqual =
          com.dremio.exec.expr.fn.impl.ArrayFunctions.areFieldReadersEqual(
              left, right, errorContext);
      resultHolder.value = areFieldReadersEqual ? 1 : 0;
    }
  }

  @FunctionTemplate(
      names = {"not_equal", "<>", "!="},
      scope = FunctionScope.SIMPLE,
      nulls = NullHandling.NULL_IF_NULL)
  public static final class ArrayNotEquals implements SimpleFunction {
    @Param FieldReader left;
    @Param FieldReader right;
    @Output NullableBitHolder resultHolder;
    @Inject FunctionErrorContext errorContext;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      if (!left.isSet() || !right.isSet()) {
        resultHolder.isSet = 0;
        return;
      }

      resultHolder.isSet = 1;
      boolean areFieldReadersEqual =
          com.dremio.exec.expr.fn.impl.ArrayFunctions.areFieldReadersEqual(
              left, right, errorContext);
      resultHolder.value = areFieldReadersEqual ? 0 : 1;
    }
  }

  public static boolean areFieldReadersEqual(
      FieldReader left, FieldReader right, FunctionErrorContext functionErrorContext) {
    return compareTo(left, right, functionErrorContext) == 0;
  }

  /** Array comparator where null appears last i.e. nulls are considered larger than all values. */
  @FunctionTemplate(
      name = FunctionGenerationHelper.COMPARE_TO_NULLS_HIGH,
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = NullHandling.INTERNAL)
  public static class ArrayCompareToNullsHigh implements SimpleFunction {
    @Param FieldReader left;
    @Param FieldReader right;
    @Output NullableIntHolder out;
    @Inject FunctionErrorContext errorContext;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      out.isSet = 1;
      if (left.isSet() && right.isSet()) {
        out.value =
            com.dremio.exec.expr.fn.impl.ArrayFunctions.compareTo(left, right, errorContext);
      } else if (left.isSet() && !right.isSet()) {
        out.value = 1;
      } else if (!left.isSet() && right.isSet()) {
        out.value = -1;
      } else {
        out.value = 0;
      }
    }
  }

  public static int compareTo(
      FieldReader left, FieldReader right, FunctionErrorContext functionErrorContext) {
    boolean leftIsValid = left instanceof UnionListReader;
    boolean rightIsValid = right instanceof UnionListReader;
    if (leftIsValid && !rightIsValid) {
      return -1;
    } else if (!leftIsValid && rightIsValid) {
      return 1;
    } else if (!leftIsValid && !rightIsValid) {
      return 0;
    } else {
      return com.dremio.exec.expr.fn.impl.comparator.ValueComparator.compareReaderValue(
          left, right);
    }
  }

  @FunctionTemplate(
      names = {"hash", "hash32"},
      scope = FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class ArrayHashCode implements SimpleFunction {
    @Param FieldReader array;
    @Output NullableIntHolder out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      out.isSet = 1;
      if (!array.isSet()) {
        out.value = 0;
      } else {
        out.value = com.dremio.exec.expr.fn.impl.ArrayFunctions.hashCodeHelper(array);
      }
    }
  }

  @FunctionTemplate(
      names = {"hash", "hash32"},
      scope = FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class ArrayHashCodeCombine implements SimpleFunction {
    @Param FieldReader array;
    @Param NullableIntHolder otherHash;
    @Output NullableIntHolder out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      out.isSet = 1;
      if ((!array.isSet()) || (otherHash.isSet == 0)) {
        out.value = 0;
      } else {
        out.value =
            com.dremio.exec.expr.fn.impl.ArrayFunctions.hashCodeHelper(array) ^ otherHash.value;
      }
    }
  }

  public static int hashCodeHelper(FieldReader array) {
    Object item = array.readObject();
    int hashCode = item.hashCode();
    return hashCode;
  }
}
