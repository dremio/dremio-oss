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
package com.dremio.dac.server;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;

import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;

import org.hibernate.validator.HibernateValidator;
import org.hibernate.validator.HibernateValidatorConfiguration;
import org.hibernate.validator.cfg.ConstraintMapping;
import org.hibernate.validator.cfg.defs.NotBlankDef;
import org.hibernate.validator.cfg.defs.NotEmptyDef;
import org.hibernate.validator.cfg.defs.NotNullDef;
import org.hibernate.validator.cfg.defs.PatternDef;

import com.dremio.dac.model.common.ValidationErrorMessages;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.spaces.Space;
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
import com.dremio.dac.proto.model.source.DB2Config;
import com.dremio.dac.proto.model.source.ElasticConfig;
import com.dremio.dac.proto.model.source.HdfsConfig;
import com.dremio.dac.proto.model.source.Host;
import com.dremio.dac.proto.model.source.MSSQLConfig;
import com.dremio.dac.proto.model.source.MongoConfig;
import com.dremio.dac.proto.model.source.MySQLConfig;
import com.dremio.dac.proto.model.source.NASConfig;
import com.dremio.dac.proto.model.source.OracleConfig;
import com.dremio.dac.proto.model.source.PostgresConfig;
import com.dremio.dac.proto.model.source.Property;
import com.dremio.dac.proto.model.source.S3Config;
import com.dremio.dac.service.errors.ClientErrorException;

/**
 * general bean validation
 */
public class InputValidation {

  private final HibernateValidatorConfiguration configuration;
  private final ConstraintMapping constraints;
  private final Validator validator;

  public InputValidation() {
    this.configuration = Validation.byProvider(HibernateValidator.class).configure();
    this.constraints = configuration.createConstraintMapping();

    configureConstraints();

    this.validator = this.configuration.addMapping(constraints)
        .buildValidatorFactory()
        .getValidator();
  }

  /**
   * definition of bean validation
   */
  private void configureConstraints() {
    configureTransforms();
    configureSources();
    configureSpace();
  }

  /**
   * constraints for Spaces
   */
  private void configureSpace() {
    constraints.type(Space.class)
      .property("name", METHOD).constraint(new PatternDef().regexp("^[^.\"]+$").message("Space name can not contain periods or double quotes"));
  }

  /**
   * constraints for Sources
   */
  private void configureSources() {
    constraints.type(SourceUI.class)
      .property("name", FIELD).constraint(new PatternDef().regexp("^[^.\"]+$").message("Source name can not contain periods or double quotes"));

    constraints.type(Host.class)
    .property("hostname", FIELD).constraint(new NotBlankDef())
    .property("port", FIELD).constraint(new NotNullDef());

    constraints.type(ElasticConfig.class)
    .property("host", FIELD).constraint(new NotEmptyDef());

    constraints.type(HdfsConfig.class)
    .property("hostname", FIELD).constraint(new NotBlankDef());

    constraints.type(MongoConfig.class)
    .property("host", FIELD).constraint(new NotEmptyDef())
    .property("useSsl", FIELD).constraint(new NotNullDef());

    constraints.type(MSSQLConfig.class)
    .property("hostname", FIELD).constraint(new NotBlankDef())
    .property("port", FIELD).constraint(new NotBlankDef());

    constraints.type(MySQLConfig.class)
    .property("hostname", FIELD).constraint(new NotBlankDef())
    .property("port", FIELD).constraint(new NotBlankDef());

    constraints.type(NASConfig.class)
    .property("path", FIELD).constraint(new NotBlankDef());

    constraints.type(OracleConfig.class)
    .property("hostname", FIELD).constraint(new NotBlankDef())
    .property("port", FIELD).constraint(new NotBlankDef())
    .property("username", FIELD).constraint(new NotBlankDef())
    .property("password", FIELD).constraint(new NotBlankDef())
    .property("instance", FIELD).constraint(new NotBlankDef());

    constraints.type(DB2Config.class)
    .property("hostname", FIELD).constraint(new NotBlankDef())
    .property("port", FIELD).constraint(new NotBlankDef())
    .property("databaseName", FIELD).constraint(new NotBlankDef());

    constraints.type(PostgresConfig.class)
    .property("hostname", FIELD).constraint(new NotBlankDef())
    .property("port", FIELD).constraint(new NotBlankDef())
    .property("databaseName", FIELD).constraint(new NotBlankDef());

    constraints.type(S3Config.class)
    .property("secure", FIELD).constraint(new NotNullDef());
    // TODO These fields are only required when there are no external buckets
    // .property("accessKey", FIELD).constraint(new NotBlankDef())
    // .property("accessSecret", FIELD).constraint(new NotBlankDef());

    constraints.type(Property.class)
    .property("name", FIELD).constraint(new NotBlankDef())
    .property("value", FIELD).constraint(new NotBlankDef());

  }

  private void configureTransforms() {
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
