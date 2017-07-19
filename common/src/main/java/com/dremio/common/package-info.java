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
/**
 * Logical (execution-engine-independent) element definitions.
 *
 * Dremio has several representations of a query including SQL, logical plans and
 * physical plans. All of the logical constructs of Dremio are separated from
 * their physical implementations in the Java execution engine. The components
 * of this package can be used to share common logical constructs between the
 * current engine and any other future alternative physical implementations of
 * the same logical constructs. This is the same for the logical expression
 * constructs defined within this package.
 *
 */
package com.dremio.common;