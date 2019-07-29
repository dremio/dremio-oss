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
package com.dremio.common.config;

public interface CommonConstants {

  /** Default (base) configuration file name.  (Classpath resource pathname.) */
  String CONFIG_DEFAULT_RESOURCE_PATHNAME = "sabot-default.conf";

  /** Module configuration files name.  (Classpath resource pathname.) */
  String SABOT_JAR_MARKER_FILE_RESOURCE_PATHNAME = "sabot-module.conf";

  /** Override configuration file name.  (Classpath resource pathname.) */
  String CONFIG_OVERRIDE_RESOURCE_PATHNAME = "sabot-override.conf";

}
