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

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.Payload;

/**
 * The last element of the annotated {@code List<String>}, i.e. the folder name, must match the
 * specified regular expression. The regular expression follows the Java regular expression
 * conventions see java.util.regex.Pattern. Accepts {@code List<String>}. null or empty elements are
 * considered valid.
 */
@Documented
@Constraint(validatedBy = FolderNamePatternValidator.class)
@Target({FIELD})
@Retention(RUNTIME)
public @interface FolderNamePattern {
  /**
   * @return the regular expression to match
   */
  String regexp();

  /**
   * @return the error message template
   */
  String message() default "{com.dremio.dac.api.FolderNamePattern.message}";

  /**
   * @return the groups the constraint belongs to
   */
  Class<?>[] groups() default {};

  /**
   * @return the payload associated to the constraint
   */
  Class<? extends Payload>[] payload() default {};
}
