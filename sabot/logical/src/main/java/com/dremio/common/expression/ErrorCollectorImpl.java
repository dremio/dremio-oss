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
package com.dremio.common.expression;

import java.util.Arrays;
import java.util.List;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.visitors.ExpressionValidationError;
import com.dremio.common.types.TypeProtos.MajorType;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

public class ErrorCollectorImpl implements ErrorCollector, AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ErrorCollectorImpl.class);
  List<ExpressionValidationError> errors;

  public ErrorCollectorImpl() {
    errors = Lists.newArrayList();
  }

  @Override
  public void addGeneralError(String s, Object... args) {
    errors.add(new ExpressionValidationError(s, args));
  }

  @Override
  public void addUnexpectedArgumentType(String name, MajorType actual, MajorType[] expected,
      int argumentIndex) {
    errors.add(
        new ExpressionValidationError(
            "Unexpected argument type. Index :%d Name: %s, Type: %s, Expected type(s): %s",
            argumentIndex, name, actual, Arrays.toString(expected)));
  }

  @Override
  public void addUnexpectedArgumentCount(int actual, Range<Integer> expected) {
    errors.add(new ExpressionValidationError("Unexpected argument count. Actual argument count: %d, Expected range: %s", actual, expected));
  }

  @Override
  public void addUnexpectedArgumentCount(int actual, int expected) {
    errors.add(new ExpressionValidationError("Unexpected argument count. Actual argument count: %d, Expected count: %d", actual, expected));
  }

  @Override
  public void addNonNumericType(MajorType actual) {
    errors.add(new ExpressionValidationError("Unexpected numeric type. Actual type: %s", actual));
  }

  @Override
  public void addUnexpectedType(int index, MajorType actual) {
    errors.add(new ExpressionValidationError("Unexpected argument type. Actual type: %s, Index: %d", actual, index));
  }

  @Override
  public void addExpectedConstantValue(int actual, String s) {
    errors.add(new ExpressionValidationError("Unexpected constant value. Name: %s, Actual: %s", s, actual));
  }

  @Override
  public boolean hasErrors() {
    return !errors.isEmpty();
  }

  @Override
  public int getErrorCount() {
    return errors.size();
  }

  @Override
  public String toErrorString() {
    return "\n" + Joiner.on("\n").join(errors);
  }

  public void close(){
      if(!errors.isEmpty()){
        throw UserException.functionError().message(Joiner.on("\n").join(errors)).build(logger);
      }
    }

  @Override
  public String toString() {
    return toErrorString();
  }

}
