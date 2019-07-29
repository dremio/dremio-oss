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
/**
 * Dremio expression materialization and evaluation facilities.
 *
 * Dremio exposes an interface for defining custom scalar and aggregate functions.
 * These functions are found by scanning the classpath at runtime and allow users
 * to add their own functions without rebuilding Dremio or changing cluster
 * configuration.
 *
 * The classes that define these functions are actually decomposed at the source
 * level, copied into generated code blocks to evaluate an entire expression
 * tree. This generated source is built at run-time as schema is discovered.
 *
 * This package contains the {@link SimpleFunction} and {@link AggrFunction}
 * interfaces that can be implemented by users to define their own aggregate
 * and scalar functions.
 */
package com.dremio.exec.expr;
