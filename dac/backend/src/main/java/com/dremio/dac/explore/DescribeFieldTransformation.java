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
package com.dremio.dac.explore;

import static java.lang.String.format;

import com.dremio.dac.explore.model.FieldTransformationBase.FieldTransformationVisitor;
import com.dremio.dac.proto.model.dataset.ConvertCase;
import com.dremio.dac.proto.model.dataset.FieldConvertCase;
import com.dremio.dac.proto.model.dataset.FieldConvertDateToNumber;
import com.dremio.dac.proto.model.dataset.FieldConvertDateToText;
import com.dremio.dac.proto.model.dataset.FieldConvertFloatToDecimal;
import com.dremio.dac.proto.model.dataset.FieldConvertFloatToInteger;
import com.dremio.dac.proto.model.dataset.FieldConvertFromJSON;
import com.dremio.dac.proto.model.dataset.FieldConvertListToText;
import com.dremio.dac.proto.model.dataset.FieldConvertNumberToDate;
import com.dremio.dac.proto.model.dataset.FieldConvertTextToDate;
import com.dremio.dac.proto.model.dataset.FieldConvertToJSON;
import com.dremio.dac.proto.model.dataset.FieldConvertToTypeIfPossible;
import com.dremio.dac.proto.model.dataset.FieldConvertToTypeWithPatternIfPossible;
import com.dremio.dac.proto.model.dataset.FieldExtract;
import com.dremio.dac.proto.model.dataset.FieldExtractList;
import com.dremio.dac.proto.model.dataset.FieldExtractMap;
import com.dremio.dac.proto.model.dataset.FieldReplaceCustom;
import com.dremio.dac.proto.model.dataset.FieldReplacePattern;
import com.dremio.dac.proto.model.dataset.FieldReplaceRange;
import com.dremio.dac.proto.model.dataset.FieldReplaceValue;
import com.dremio.dac.proto.model.dataset.FieldSimpleConvertToType;
import com.dremio.dac.proto.model.dataset.FieldSplit;
import com.dremio.dac.proto.model.dataset.FieldTrim;
import com.dremio.dac.proto.model.dataset.FieldUnnestList;
import com.dremio.dac.proto.model.dataset.TransformField;
import com.dremio.dac.proto.model.dataset.TrimType;

class DescribeFieldTransformation extends FieldTransformationVisitor<String> {

  private final TransformField field;

  public DescribeFieldTransformation(TransformField field) {
    super();
    this.field = field;
  }

  @Override
  public String visit(FieldConvertCase convertCase) throws Exception {
    return describeConvertCase(convertCase.getConvertCase(), field.getSourceColumnName());
  }

  @Override
  public String visit(FieldTrim trim) throws Exception {
    return describeTrim(field.getSourceColumnName(), trim.getTrimType());
  }

  @Override
  public String visit(FieldExtract extract) throws Exception {
    return "Extract " + ExtractRecommender.describe(extract.getRule()) + " from " + field.getSourceColumnName();
  }

  @Override
  public String visit(FieldConvertFloatToInteger floatToInt) throws Exception {
    return format("Convert %s from Float to Integer", field.getSourceColumnName());
  }

  @Override
  public String visit(FieldConvertFloatToDecimal floatToDec) throws Exception {
    return format("Convert %s from Float to Decimal", field.getSourceColumnName());
  }

  @Override
  public String visit(FieldConvertDateToText dateToText) throws Exception {
    return format("Convert %s from Date to Text", field.getSourceColumnName());
  }

  @Override
  public String visit(FieldConvertNumberToDate numberToDate) throws Exception {
    return format("Convert %s from Number to Date", field.getSourceColumnName());
  }

  @Override
  public String visit(FieldConvertDateToNumber dateToNumber) throws Exception {
    return format("Convert %s from Date to Number", field.getSourceColumnName());
  }

  @Override
  public String visit(FieldConvertTextToDate textToDate) throws Exception {
    return format("Convert %s from Text to Date", field.getSourceColumnName());
  }

  @Override
  public String visit(FieldConvertListToText listToText) throws Exception {
    return format("Convert %s from List to Text", field.getSourceColumnName());
  }

  @Override
  public String visit(FieldConvertToJSON toJson) throws Exception {
    return format("Convert %s to JSON", field.getSourceColumnName());
  }

  @Override
  public String visit(FieldUnnestList unnest) throws Exception {
    return format("Unnest %s", field.getSourceColumnName());
  }

  @Override
  public String visit(FieldReplacePattern replacePattern) throws Exception {
    return format("Replace in %s", field.getSourceColumnName());
  }

  @Override
  public String visit(FieldReplaceCustom replaceCustom) throws Exception {
    return format("Replace %s", field.getSourceColumnName());
  }

  @Override
  public String visit(FieldReplaceValue replaceValue) throws Exception {
    return format("Replace values in %s", field.getSourceColumnName());
  }

  @Override
  public String visit(FieldReplaceRange replaceRange) throws Exception {
    return format("Replace range %s, %s in %s", replaceRange.getLowerBound(), replaceRange.getUpperBound(),
        field.getSourceColumnName());
  }

  @Override
  public String visit(FieldExtractMap extract) throws Exception {
    return "Extract map";
  }

  @Override
  public String visit(FieldExtractList extract) throws Exception {
    return "Extract list";
  }

  @Override
  public String visit(FieldSplit split) throws Exception {
    return format("Split %s on '%s'", field.getSourceColumnName(), split.getRule().getPattern());
  }

  @Override
  public String visit(FieldSimpleConvertToType toType) throws Exception {
    return format("Convert %s to %s", field.getSourceColumnName(), toType.getDataType().name().toLowerCase());
  }

  @Override
  public String visit(FieldConvertToTypeIfPossible toTypeIfPossible) throws Exception {
    return format("Convert %s to %s", field.getSourceColumnName(),
        toTypeIfPossible.getDesiredType().name().toLowerCase());
  }

  @Override
  public String visit(FieldConvertToTypeWithPatternIfPossible toTypeIfPossible) throws Exception {
    return format("Convert %s to %s with pattern %s", field.getSourceColumnName(),
      toTypeIfPossible.getDesiredType().name().toLowerCase(), toTypeIfPossible.getPattern());
  }

  @Override
  public String visit(FieldConvertFromJSON fromJson) throws Exception {
    return format("Auto-detect JSON in %s", field.getSourceColumnName());
  }


  private static String describeConvertCase(ConvertCase c, String columnName) {
    String display;
    switch (c) {
    case LOWER_CASE:
      display = "lower case";
      break;
    case UPPER_CASE:
      display = "upper case";
      break;
    case TITLE_CASE:
      display = "title case";
      break;
    default:
      throw new UnsupportedOperationException("Unknown case " + c);
    }
    return "Convert case of " + columnName + " to " + display;
  }


  private static String describeTrim(String columnName, TrimType trimType) {
    String display;
    switch (trimType) {
    case BOTH:
      display = "on both sides";
      break;
    case LEFT:
      display = "on the left";
      break;
    case RIGHT:
      display = "on the right";
      break;
    default:
      throw new UnsupportedOperationException("Unknown trim " + trimType);
    }
    return "Trim " + columnName + " " + display;
  }
}
