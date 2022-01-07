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

import static java.lang.annotation.ElementType.FIELD;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;

import org.hibernate.validator.HibernateValidator;
import org.hibernate.validator.HibernateValidatorConfiguration;
import org.hibernate.validator.cfg.ConstraintMapping;
import org.hibernate.validator.cfg.defs.NotBlankDef;
import org.hibernate.validator.cfg.defs.NotEmptyDef;
import org.hibernate.validator.cfg.defs.NotNullDef;

import com.dremio.dac.model.common.ValidationErrorMessages;
import com.dremio.dac.proto.model.dataset.Dimension;
import com.dremio.dac.proto.model.dataset.FieldConvertTextToDate;
import com.dremio.dac.proto.model.dataset.Measure;
import com.dremio.dac.proto.model.dataset.TransformAddCalculatedField;
import com.dremio.dac.proto.model.dataset.TransformConvertToSingleType;
import com.dremio.dac.proto.model.dataset.TransformDrop;
import com.dremio.dac.proto.model.dataset.TransformField;
import com.dremio.dac.proto.model.dataset.TransformFilter;
import com.dremio.dac.proto.model.dataset.TransformGroupBy;
import com.dremio.dac.proto.model.dataset.TransformRename;
import com.dremio.dac.proto.model.dataset.TransformSort;
import com.dremio.dac.proto.model.dataset.TransformSorts;
import com.dremio.dac.proto.model.dataset.TransformSplitByDataType;
import com.dremio.dac.proto.model.dataset.TransformUpdateSQL;
import com.dremio.dac.service.errors.ClientErrorException;

/**
 * general bean validation
 */
public class InputValidation {

  private final Validator validator;

  public InputValidation() {

    HibernateValidatorConfiguration config = Validation.byProvider(HibernateValidator.class).configure();
    this.validator = config
        .addMapping(configureTransforms(config.createConstraintMapping()))
        .buildValidatorFactory()
        .getValidator();
  }


  private ConstraintMapping configureTransforms(ConstraintMapping constraints) {
    constraints.type(TransformSort.class)
    .property("sortedColumnName", FIELD).constraint(new NotBlankDef());
    //  order

    constraints.type(TransformSorts.class)
    .property("columns", FIELD).constraint(new NotEmptyDef());

    constraints.type(TransformDrop.class)
    .property("droppedColumnName", FIELD).constraint(new NotBlankDef());

    constraints.type(TransformRename.class)
    .property("oldColumnName", FIELD).constraint(new NotBlankDef())
    .property("newColumnName", FIELD).constraint(new NotBlankDef());

    constraints.type(TransformAddCalculatedField.class)
    .property("newColumnName", FIELD).constraint(new NotBlankDef())
    .property("expression", FIELD).constraint(new NotBlankDef());

    constraints.type(TransformUpdateSQL.class)
    .property("sql", FIELD).constraint(new NotBlankDef());

    constraints.type(TransformField.class)
    .property("sourceColumnName", FIELD).constraint(new NotBlankDef())
    .property("fieldTransformation", FIELD).constraint(new NotNullDef());
    //    newColumnName
    //    dropSourceColumn

    constraints.type(FieldConvertTextToDate.class)
    .property("format", FIELD).constraint(new NotBlankDef());
    // optional but one of the date/time types:
    // .property("desiredType", FIELD).constraint(new ());

    constraints.type(TransformConvertToSingleType.class)
    .property("sourceColumnName", FIELD).constraint(new NotBlankDef())
    .property("newColumnName", FIELD).constraint(new NotBlankDef())
    .property("dropSourceColumn", FIELD).constraint(new NotNullDef())
    .property("desiredType", FIELD).constraint(new NotNullDef())
    .property("castWhenPossible", FIELD).constraint(new NotNullDef())
    .property("actionForNonMatchingValue", FIELD).constraint(new NotNullDef());
    //      defaultValue // optional

    constraints.type(TransformSplitByDataType.class)
    .property("sourceColumnName", FIELD).constraint(new NotBlankDef())
    .property("newColumnNamePrefix", FIELD).constraint(new NotBlankDef())
    .property("dropSourceColumn", FIELD).constraint(new NotNullDef())
    .property("selectedTypes", FIELD).constraint(new NotEmptyDef());

    constraints.type(TransformFilter.class)
    .property("sourceColumnName", FIELD).constraint(new NotBlankDef())
    .property("filter", FIELD).constraint(new NotNullDef());
    //      keepNull
    //        exclude

    constraints.type(Dimension.class)
    .property("column", FIELD).constraint(new NotBlankDef());

    constraints.type(Measure.class)
    .property("column", FIELD).constraint(new NotBlankDef())
    .property("type", FIELD).constraint(new NotNullDef());

    constraints.type(TransformGroupBy.class).constraint(new TransformGroupByConstraintDef());

    return constraints;
  }

  public boolean isValidExtension(String extension) {
    //pattern of length >= 1 for any alphanumeric character
    final Pattern pattern = Pattern.compile("[a-zA-Z0-9]+");
    final Matcher matcher = pattern.matcher(extension);
    return matcher.matches();
  }

  /**
   * validate the input
   * @param o the object to validate
   */
  public void validate(Object o) throws ClientErrorException {
    Set<ConstraintViolation<Object>> errors = validator.validate(o);
    if (errors != null && errors.size() != 0) {
      ValidationErrorMessages messages = new ValidationErrorMessages();
      StringBuilder message = new StringBuilder("Invalid input ");
      for (ConstraintViolation<Object> constraintViolation : errors) {
        String field = constraintViolation.getPropertyPath().toString();
        messages.addFieldError(field, constraintViolation.getMessage());
        message.append("\n" + field + ": " + constraintViolation.getMessage());
      }
      throw new ClientErrorException(message.toString(), messages);
    }
  }
}
