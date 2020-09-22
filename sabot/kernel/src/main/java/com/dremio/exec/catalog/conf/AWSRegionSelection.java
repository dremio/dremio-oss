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
package com.dremio.exec.catalog.conf;

import io.protostuff.Tag;

/**
 * AWS Region Name
 */
public enum AWSRegionSelection {
  @Tag(1) @DisplayMetadata(label = "US East (Ohio) us-east-2") US_EAST_2("us-east-2"),
  @Tag(2) @DisplayMetadata(label = "US East (N. Virginia) us-east-1") US_EAST_1("us-east-1"),
  @Tag(3) @DisplayMetadata(label = "US West (N. California) us-west-1") US_WEST_1("us-west-1"),
  @Tag(4) @DisplayMetadata(label = "US West (Oregon) us-west-2") US_WEST_2("us-west-2"),
  @Tag(5) @DisplayMetadata(label = "Africa (Cape Town) af-south-1") AF_SOUTH_1("af-south-1"),
  @Tag(6) @DisplayMetadata(label = "Asia Pacific (Hong Kong) ap-east-1") AP_EAST_1("ap-east-1"),
  @Tag(7) @DisplayMetadata(label = "Asia Pacific (Mumbai) ap-south-1") AP_SOUTH_1("ap-south-1"),
  @Tag(8) @DisplayMetadata(label = "Asia Pacific (Osaka-Local) ap-northeast-3") AP_NORTHEAST_3("ap-northeast-3"),
  @Tag(9) @DisplayMetadata(label = "Asia Pacific (Seoul) ap-northeast-2") AP_NORTHEAST_2("ap-northeast-2"),
  @Tag(10) @DisplayMetadata(label = "Asia Pacific (Singapore) ap-southeast-1") AP_SOUTHEAST_1("ap-southeast-1"),
  @Tag(11) @DisplayMetadata(label = "Asia Pacific (Sydney) ap-southeast-2") AP_SOUTHEAST_2("ap-southeast-2"),
  @Tag(12) @DisplayMetadata(label = "Asia Pacific (Tokyo) ap-northeast-1") AP_NORTHEAST_1("ap-northeast-1"),
  @Tag(13) @DisplayMetadata(label = "Canada (Central) ca-central-1") CA_CENTRAL_1("ca-central-1"),
  @Tag(14) @DisplayMetadata(label = "China (Beijing) ca-north-1") CA_NORTH_1("ca-north-1"),
  @Tag(15) @DisplayMetadata(label = "China (Ningxia) ca-northwest-1") CA_NORTHWEST_1("ca-northwest-1"),
  @Tag(16) @DisplayMetadata(label = "Europe (Frankfurt) eu-central-1") EU_CENTRAL_1("eu-central-1"),
  @Tag(17) @DisplayMetadata(label = "Europe (Ireland) eu-west-1") EU_WEST_1("eu-west-1"),
  @Tag(18) @DisplayMetadata(label = "Europe (London) eu-west-2") EU_WEST_2("eu-west-2"),
  @Tag(19) @DisplayMetadata(label = "Europe (Milan) eu-south-1") EU_SOUTH_1("eu-south-1"),
  @Tag(20) @DisplayMetadata(label = "Europe (Paris) eu-west-3") EU_WEST_3("eu-west-3"),
  @Tag(21) @DisplayMetadata(label = "Europe (Stockholm) eu-north-1") EU_NORTH_1("eu-north-1"),
  @Tag(22) @DisplayMetadata(label = "Middle East (Bahrain) me-south-1") ME_SOUTH_1("me-south-1"),
  @Tag(23) @DisplayMetadata(label = "South America (São Paulo) sa-east-1") SA_EAST_1("sa-east-1"),
  @Tag(24) @DisplayMetadata(label = "AWS GovCloud (US-East) us-gov-east-1") US_GOV_EAST_1("us-gov-east-1"),
  @Tag(25) @DisplayMetadata(label = "AWS GovCloud (US) us-gov-west-1") US_GOV_WEST_1("us-gov-west-1");

  private final String endPoint;

  AWSRegionSelection(String endPoint) {
    this.endPoint = endPoint;
  }

  public String getRegionName() {
    return this.endPoint;
  }
}
