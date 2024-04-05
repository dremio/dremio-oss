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

import com.dremio.common.expression.visitors.ExprVisitor;
import com.dremio.common.types.Types;
import com.google.common.collect.Iterators;
import java.util.Iterator;

public class ConvertExpression extends LogicalExpressionBase
    implements Iterable<LogicalExpression> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConvertExpression.class);

  public static final String CONVERT_FROM = "convert_from";
  public static final String CONVERT_TO = "convert_to";

  private final LogicalExpression input;
  private final CompleteType type;
  private final String convertFunction;
  private final String encodingType;

  /**
   * @param encodingType
   * @param convertFunction
   * @param input
   */
  public ConvertExpression(String convertFunction, String encodingType, LogicalExpression input) {
    this.input = input;
    this.convertFunction =
        CONVERT_FROM.equals(convertFunction.toLowerCase()) ? CONVERT_FROM : CONVERT_TO;
    this.encodingType = encodingType.toUpperCase();
    this.type =
        CompleteType.fromMinorType(
            Types.getMinorTypeFromName(encodingType.split("_", 2)[0].toLowerCase()));
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitConvertExpression(this, value);
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.singletonIterator(input);
  }

  @Override
  public int getSizeOfChildren() {
    return 1;
  }

  public String getConvertFunction() {
    return convertFunction;
  }

  public LogicalExpression getInput() {
    return input;
  }

  @Override
  public CompleteType getCompleteType() {
    return type;
  }

  public String getEncodingType() {
    return encodingType;
  }

  @Override
  public String toString() {
    return "ConvertExpression [input="
        + input
        + ", type="
        + type.toString()
        + ", convertFunction="
        + convertFunction
        + ", conversionType="
        + encodingType
        + "]";
  }
}
