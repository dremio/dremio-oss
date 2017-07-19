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
 * Java client API for submitting queries and accepting result sets from a Dremio
 * server.
 *
 * Data returned to this client is stored in the default columnar Value Vector
 * data structures used by Dremio internally. For users of Dremio requiring a more
 * traditional database interface to consume from an application, a JDBC driver
 * is available as well; see the {@see com.dremio.jdbc} package.
 */
package com.dremio.exec.client;