/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.sabot.rpc.user;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.dremio.exec.proto.UserProtos.RecordBatchFormat;
import com.dremio.exec.proto.UserProtos.RecordBatchType;
import com.dremio.exec.proto.UserProtos.UserToBitHandshake;

/**
 * Test some of {@code UserRPCServer} helper methods
 */
public class TestUserRpcServer {

  @Test
  public void testChooseDremioRecordBatchFormat() {
    UserToBitHandshake handshake = UserToBitHandshake.newBuilder()
        .setRecordBatchType(RecordBatchType.DREMIO)
        .addSupportedRecordBatchFormats(RecordBatchFormat.DREMIO_0_9)
        .addSupportedRecordBatchFormats(RecordBatchFormat.DREMIO_1_4)
        .build();

    assertEquals(RecordBatchFormat.DREMIO_1_4, UserRPCServer.chooseDremioRecordBatchFormat(handshake));
  }

  @Test
  public void testChooseDremioRecordBatchFormatWithUnknownValue() {
    UserToBitHandshake handshake = UserToBitHandshake.newBuilder()
        .setRecordBatchType(RecordBatchType.DREMIO)
        // simulating protobuf handling a unknown enum value during decoding
        .addSupportedRecordBatchFormats(RecordBatchFormat.UNKNOWN)
        .addSupportedRecordBatchFormats(RecordBatchFormat.DREMIO_0_9)
        .addSupportedRecordBatchFormats(RecordBatchFormat.DREMIO_1_4)
        .build();

    assertEquals(RecordBatchFormat.DREMIO_1_4, UserRPCServer.chooseDremioRecordBatchFormat(handshake));
  }

  @Test
  public void testChooseDremioRecordBatchFormatOlderClient() {
    UserToBitHandshake handshake = UserToBitHandshake.newBuilder()
        .setRecordBatchType(RecordBatchType.DREMIO)
        .build();

    assertEquals(RecordBatchFormat.DREMIO_0_9, UserRPCServer.chooseDremioRecordBatchFormat(handshake));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testChooseDremioRecordBatchFormatWithOnlyUnknownValue() {
    UserToBitHandshake handshake = UserToBitHandshake.newBuilder()
        .setRecordBatchType(RecordBatchType.DREMIO)
        // simulating protobuf handling a unknown enum value during decoding
        .addSupportedRecordBatchFormats(RecordBatchFormat.UNKNOWN)
        .build();

    UserRPCServer.chooseDremioRecordBatchFormat(handshake);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testChooseDremioRecordBatchFormatWithInvalidValue() {
    UserToBitHandshake handshake = UserToBitHandshake.newBuilder()
        .setRecordBatchType(RecordBatchType.DREMIO)
        // invalid value client should never send
        .addSupportedRecordBatchFormats(RecordBatchFormat.DRILL_1_0)
        .addSupportedRecordBatchFormats(RecordBatchFormat.DREMIO_0_9)
        .build();

    UserRPCServer.chooseDremioRecordBatchFormat(handshake);
  }
}
