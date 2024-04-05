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
package com.dremio.provision;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

/** Validate correct provisioning type properties to cluster type. */
public interface ConsistentProps {

  ClusterType getClusterType();

  AwsPropsApi getAwsProps();

  YarnPropsApi getYarnProps();

  /** Validator for provisioning */
  public static class Validator implements ConstraintValidator<Annotation, ConsistentProps> {

    @Override
    public boolean isValid(ConsistentProps value, ConstraintValidatorContext context) {
      if (value.getClusterType() == ClusterType.EC2) {
        return value.getAwsProps() != null && value.getYarnProps() == null;
      } else if (value.getClusterType() == ClusterType.YARN) {
        return value.getAwsProps() == null && value.getYarnProps() != null;
      }
      return false;
    }
  }

  /** Annotation for validator */
  @Constraint(validatedBy = Validator.class)
  @Target({ElementType.TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  public @interface Annotation {
    Class<?>[] groups() default {};

    String message() default
        "Type must be EC2 with AwsProps populated or YARN with YarnProps populated.";

    Class<? extends Payload>[] payload() default {};
  }
}
