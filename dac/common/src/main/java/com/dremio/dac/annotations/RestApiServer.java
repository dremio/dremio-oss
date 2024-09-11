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
package com.dremio.dac.annotations;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Annotation for REST API server classes derived from jersey's ResourceConfig. The server class
 * must have a public constructor with only parameter of type ScanResult or the constructor w/o
 * parameters. When both are present, the one with ScanResult is chosen.
 *
 * <p>{@link Inherited} below makes the annotation inherited by derived classes. This, however, is
 * not picked up by reflections.org class scanner, so derived server classes must be annotated.
 */
@Retention(RUNTIME)
@Target(TYPE)
@Inherited
public @interface RestApiServer {
  /** Path spec to use for the server. For example: "/api/v4/*". */
  String pathSpec();

  /**
   * Comma-separated tags that allow filtering of unintended scanned classes. The factory class
   * takes allowed tags and only allows classes that have one of those tags.
   */
  String tags();
}
