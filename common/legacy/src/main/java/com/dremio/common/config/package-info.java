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
 * Internal configuration state definitions.
 *
 * Overall structure allows for configurations to be nested within one another
 * to provide hierarchical organization of configuration options. Sabot configurations
 * are stored in a JSON format in files named sabot-override.conf available on the classpath.
 * See the SabotConfig class for more information on config file loading.
 */
package com.dremio.common.config;
