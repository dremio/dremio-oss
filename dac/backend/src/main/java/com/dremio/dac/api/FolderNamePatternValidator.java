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
package com.dremio.dac.api;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.PatternSyntaxException;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.validator.constraintvalidation.HibernateConstraintValidatorContext;
import org.hibernate.validator.internal.engine.messageinterpolation.util.InterpolationHelper;

public class FolderNamePatternValidator
    implements ConstraintValidator<FolderNamePattern, List<String>> {
  private java.util.regex.Pattern pattern;
  private String escapedRegexp;

  @Override
  public void initialize(FolderNamePattern parameters) {
    try {
      pattern = java.util.regex.Pattern.compile(parameters.regexp());
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(e);
    }

    escapedRegexp = InterpolationHelper.escapeMessageParameter(parameters.regexp());
  }

  @Override
  public boolean isValid(
      List<String> value, ConstraintValidatorContext constraintValidatorContext) {
    if (CollectionUtils.isEmpty(value)) {
      return true;
    }

    if (constraintValidatorContext instanceof HibernateConstraintValidatorContext) {
      constraintValidatorContext
          .unwrap(HibernateConstraintValidatorContext.class)
          .addMessageParameter("regexp", escapedRegexp);
    }

    Matcher m = pattern.matcher(value.get(value.size() - 1));
    return m.matches();
  }
}
