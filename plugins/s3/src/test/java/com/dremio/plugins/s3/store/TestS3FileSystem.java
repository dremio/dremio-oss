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
package com.dremio.plugins.s3.store;

import org.junit.Assert;
import org.junit.Test;

import software.amazon.awssdk.regions.Region;

/**
 * Test the S3FileSystem class.
 */
public class TestS3FileSystem {
  @Test
  public void testValidRegionFromEndpoint() {
    Region r = S3FileSystem.getAwsRegionFromEndpoint("s3-eu-central-1.amazonaws.com");
    Assert.assertEquals(Region.EU_CENTRAL_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3-us-gov-west-1.amazonaws.com");
    Assert.assertEquals(Region.US_GOV_WEST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3.ap-southeast-1.amazonaws.com");
    Assert.assertEquals(Region.AP_SOUTHEAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3.dualstack.ca-central-1.amazonaws.com");
    Assert.assertEquals(Region.CA_CENTRAL_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("dremio.s3-control.cn-north-1.amazonaws.com.cn");
    Assert.assertEquals(Region.CN_NORTH_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("accountId.s3-control.dualstack.eu-central-1.amazonaws.com");
    Assert.assertEquals(Region.EU_CENTRAL_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3-accesspoint.eu-west-1.amazonaws.com");
    Assert.assertEquals(Region.EU_WEST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3-accesspoint.dualstack.sa-east-1.amazonaws.com");
    Assert.assertEquals(Region.SA_EAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3-fips.us-gov-west-1.amazonaws.com");
    Assert.assertEquals(Region.US_GOV_WEST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3.dualstack.us-gov-east-1.amazonaws.com");
    Assert.assertEquals(Region.US_GOV_EAST_1, r);
  }

  @Test
  public void testInvalidRegionFromEndpoint() {
    Region r = S3FileSystem.getAwsRegionFromEndpoint("us-west-1");
    Assert.assertEquals(Region.US_EAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("s3-eu-central-1");
    Assert.assertEquals(Region.US_EAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("abc");
    Assert.assertEquals(Region.US_EAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint("");
    Assert.assertEquals(Region.US_EAST_1, r);

    r = S3FileSystem.getAwsRegionFromEndpoint(null);
    Assert.assertEquals(Region.US_EAST_1, r);
  }
}
