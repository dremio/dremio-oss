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
package com.dremio.common.utils.protos;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.protostuff.ByteString;
import io.protostuff.Output;
import io.protostuff.Schema;
import java.io.IOException;
import java.nio.ByteBuffer;

/***
 * Protostuff Output for hashing an object
 */
public class HasherOutput implements Output {
  private final Hasher hasher;

  public HasherOutput(Hasher hasher) {
    this.hasher = hasher;
  }

  public HasherOutput() {
    this.hasher = Hashing.sha256().newHasher();
  }

  @Override
  public void writeInt32(int fieldNumber, int value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putInt(value);
  }

  @Override
  public void writeUInt32(int fieldNumber, int value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putInt(value);
  }

  @Override
  public void writeSInt32(int fieldNumber, int value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putInt(value);
  }

  @Override
  public void writeFixed32(int fieldNumber, int value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putInt(value);
  }

  @Override
  public void writeSFixed32(int fieldNumber, int value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putInt(value);
  }

  @Override
  public void writeInt64(int fieldNumber, long value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putLong(value);
  }

  @Override
  public void writeUInt64(int fieldNumber, long value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putLong(value);
  }

  @Override
  public void writeSInt64(int fieldNumber, long value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putLong(value);
  }

  @Override
  public void writeFixed64(int fieldNumber, long value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putLong(value);
  }

  @Override
  public void writeSFixed64(int fieldNumber, long value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putLong(value);
  }

  @Override
  public void writeFloat(int fieldNumber, float value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putFloat(value);
  }

  @Override
  public void writeDouble(int fieldNumber, double value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putDouble(value);
  }

  @Override
  public void writeBool(int fieldNumber, boolean value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putBoolean(value);
  }

  @Override
  public void writeEnum(int fieldNumber, int value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putInt(value);
  }

  @Override
  public void writeString(int fieldNumber, String value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putUnencodedChars(value);
  }

  @Override
  public void writeBytes(int fieldNumber, ByteString value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putBytes(value.asReadOnlyByteBuffer());
  }

  @Override
  public void writeByteArray(int fieldNumber, byte[] value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putBytes(value);
  }

  @Override
  public void writeByteRange(
      boolean utf8String, int fieldNumber, byte[] value, int offset, int length, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putBytes(value, offset, length);
  }

  @Override
  public <T> void writeObject(int fieldNumber, T value, Schema<T> schema, boolean repeated)
      throws IOException {
    hasher.putInt(fieldNumber);
    hasher.putUnencodedChars(schema.messageFullName());
    schema.writeTo(this, value);
  }

  @Override
  public void writeBytes(int fieldNumber, ByteBuffer value, boolean repeated) {
    hasher.putInt(fieldNumber);
    hasher.putBytes(value);
  }

  public Hasher getHasher() {
    return hasher;
  }
}
