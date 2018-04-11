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
package com.dremio.datastore;


import java.io.IOException;

import org.apache.commons.codec.binary.Base64;

/**
 * A NOP serializer
 */
public class PassThroughSerializer extends Serializer<byte[]> {
  private static final PassThroughSerializer INSTANCE = new PassThroughSerializer();

  public PassThroughSerializer() {
  }

  @Override
  public byte[] convert(byte[] bytes) {
    return bytes;
  }

  @Override
  public byte[] revert(byte[] bytes) {
    return bytes;
  }

  @Override
  public String toJson(byte[] v) throws IOException {
    return Base64.encodeBase64String(v);
  }

  @Override
  public byte[] fromJson(String v) throws IOException {
    return Base64.decodeBase64(v);
  }

  public static Serializer<byte[] > instance() {
    return INSTANCE;
  }
}
