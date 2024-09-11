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

package com.dremio.exec.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class TestProtoSerializer {
  @Test
  public void testLargeStringLength() throws Exception {
    String plan = new String(new char[10]).replace('\0', '0');
    String profile = String.format("{\"jsonPlan\": \"%s\"}", plan);
    InstanceSerializer<QueryProfile> serializer = ProtoSerializer.of(QueryProfile.class, 10);
    QueryProfile prof = serializer.deserialize(profile.getBytes(StandardCharsets.UTF_8));
    assertEquals(prof.getJsonPlan(), plan);
  }

  @Test
  public void testIgnoreUnknownProperties() throws Exception {
    String profile = "{\"jsonPlan\": \"0000\", \"blabla\": \"1\"}";
    InstanceSerializer<QueryProfile> serializer = ProtoSerializer.of(QueryProfile.class, 500);
    QueryProfile prof = serializer.deserialize(profile.getBytes(StandardCharsets.UTF_8));
    assertEquals(prof.getJsonPlan(), "0000");
  }

  @Test
  public void testStringLengthLimitExceed() {
    String plan = new String(new char[10]).replace('\0', '0');
    String profile = String.format("{\"jsonPlan\": \"%s\"}", plan);
    InstanceSerializer<QueryProfile> serializer = ProtoSerializer.of(QueryProfile.class, 9);
    assertThrows(
        StreamConstraintsException.class,
        () -> serializer.deserialize(profile.getBytes(StandardCharsets.UTF_8)));
  }
}
