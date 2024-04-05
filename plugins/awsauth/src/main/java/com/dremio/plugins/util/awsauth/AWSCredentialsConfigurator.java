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

package com.dremio.plugins.util.awsauth;

import com.dremio.exec.catalog.conf.Property;
import java.util.List;

/** Helper to configure credentials to S3 and AWS Glue. */
@FunctionalInterface
public interface AWSCredentialsConfigurator {

  /**
   * Configures storage plugin by setting credentials properties used to connect to S3 and AWS Glue.
   *
   * @param properties storage plugin properties to be modified
   * @return the credentials provider type used to connect to S3 or AWS Glue.
   */
  String configureCredentials(List<Property> properties);
}
