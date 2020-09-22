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
package com.dremio;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.immutables.value.Value.Style;

import com.dremio.common.SentinelSecure;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Describes Dremio's default annotation style for Immutables.
 *
 * Keep in sync with {@link ValidatingGnarlyStyle}.
 */
@Target({ElementType.PACKAGE, ElementType.TYPE})
@Retention(RetentionPolicy.CLASS)
@JsonSerialize
@Style(
    builder = "new",
    init = "set*",
    typeImmutable = "Immutable*",
    validationMethod = Style.ValidationMethod.NONE,
    additionalJsonAnnotations = {SentinelSecure.class}
    )
public @interface GnarlyStyle {}
