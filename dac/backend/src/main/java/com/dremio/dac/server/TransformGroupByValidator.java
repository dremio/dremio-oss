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
package com.dremio.dac.server;

import java.util.List;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import com.dremio.dac.proto.model.dataset.TransformGroupBy;

/**
 * Validator for TransformGroupByConstraint
 * TODO: make it generic
 */
public class TransformGroupByValidator implements ConstraintValidator<TransformGroupByConstraint, TransformGroupBy> {

  @Override
  public void initialize(TransformGroupByConstraint constraintAnnotation) {
  }

  @Override
  public boolean isValid(TransformGroupBy groupBy, ConstraintValidatorContext context) {
    if (isEmpty(groupBy.getColumnsDimensionsList()) && isEmpty(groupBy.getColumnsMeasuresList())) {
      return false;
    }
    return true;
  }

  public boolean isEmpty(List<?> list) {
    if (list == null || list.isEmpty()) {
      return true;
    }
    return false;
  }
}

