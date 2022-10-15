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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import io.protostuff.ByteString;
import io.protostuff.Output;
import io.protostuff.Schema;

/**
 * Black list fields when writing to a protostuff Output.  This currently only works with HashingOutput
 *
 * An instance of class is need per thread.
 */
public class BlackListOutput implements Output {
  private static final int[] EmptyArray = new int[0];
  private final Output delegate;
  private final Map<String, int[]> schemaNameToBlackList;
  @VisibleForTesting
  int[] currentBlackList;

  @VisibleForTesting
  BlackListOutput(
    Output delegate, Map<String, int[]> schemaNameToBlackList, int[] currentBlackList) {
    this.delegate = delegate;
    this.schemaNameToBlackList = schemaNameToBlackList;
    this.currentBlackList = currentBlackList;
  }

  @Override
  public void writeInt32(int fieldNumber, int value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeInt32(fieldNumber, value, repeated);
  }

  @Override
  public void writeUInt32(int fieldNumber, int value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeUInt32(fieldNumber, value, repeated);
  }

  @Override
  public void writeSInt32(int fieldNumber, int value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeSInt32(fieldNumber, value, repeated);
  }

  @Override
  public void writeFixed32(int fieldNumber, int value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeFixed32(fieldNumber, value, repeated);
  }

  @Override
  public void writeSFixed32(int fieldNumber, int value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeSFixed32(fieldNumber, value, repeated);
  }

  @Override
  public void writeInt64(int fieldNumber, long value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeInt64(fieldNumber, value, repeated);
  }

  @Override
  public void writeUInt64(int fieldNumber, long value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeUInt64(fieldNumber, value, repeated);
  }

  @Override
  public void writeSInt64(int fieldNumber, long value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeSInt64(fieldNumber, value, repeated);
  }

  @Override
  public void writeFixed64(int fieldNumber, long value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeFixed64(fieldNumber, value, repeated);
  }

  @Override
  public void writeSFixed64(int fieldNumber, long value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeSFixed64(fieldNumber, value, repeated);
  }

  @Override
  public void writeFloat(int fieldNumber, float value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeFloat(fieldNumber, value, repeated);
  }

  @Override
  public void writeDouble(int fieldNumber, double value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeDouble(fieldNumber, value, repeated);
  }

  @Override
  public void writeBool(int fieldNumber, boolean value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeBool(fieldNumber, value, repeated);
  }

  @Override
  public void writeEnum(int fieldNumber, int value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeEnum(fieldNumber, value, repeated);
  }

  @Override
  public void writeString(int fieldNumber, String value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeString(fieldNumber, value, repeated);
  }

  @Override
  public void writeBytes(int fieldNumber, ByteString value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeBytes(fieldNumber, value, repeated);
  }

  @Override
  public void writeByteArray(int fieldNumber, byte[] value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeByteArray(fieldNumber, value, repeated);
  }

  @Override
  public void writeByteRange(boolean utf8String, int fieldNumber, byte[] value, int offset, int length, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeByteRange(utf8String, fieldNumber, value, offset, length, repeated);
  }

  @Override
  public <T> void writeObject(int fieldNumber, T value, Schema<T> schema, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    int[] parentBlackList = currentBlackList;
    try {
      //HACK the protostuff api ping pongs back and fourth between the schema doing flow control and the output
      //This makes it impossible to do a robust wrapper pattern
      String messageFullName = schema.messageFullName();
      currentBlackList = schemaNameToBlackList.getOrDefault(messageFullName, EmptyArray);
      writeString(fieldNumber * -1, messageFullName, repeated);
      schema.writeTo(this, value);
      writeString(fieldNumber * -1, schema.messageName(), repeated);
    } finally {
      currentBlackList = parentBlackList;
    }
  }

  @Override
  public void writeBytes(int fieldNumber, ByteBuffer value, boolean repeated) throws IOException {
    if(isBlackListed(fieldNumber)){
      return;
    }
    delegate.writeBytes(fieldNumber, value, repeated);
  }

  private boolean isBlackListed(int fieldNumber){
    for (int i = 0; i < currentBlackList.length; i++) {
      if(currentBlackList[i] == fieldNumber){
        return true;
      }
    }
    return false;
  }

  public static class Builder {
    private final Map<String, int[]> blackListed = new HashMap<>();

    public Builder blacklist(Schema<?> schema, String...fields){
      int[] blackListFieldsNumbers = new int[fields.length];
      if(blackListed.containsKey(schema.messageFullName())){
        throw new RuntimeException(schema.messageFullName());
      } else {
        blackListed.put(schema.messageFullName(), blackListFieldsNumbers);
      }
      for (int i = 0; i < fields.length; i++) {
        String field = fields[i];
        int fieldNumber = schema.getFieldNumber(field);
        if(fieldNumber == 0) {
          throw new RuntimeException(schema.messageFullName() + ":" + field);
        }  else {
          blackListFieldsNumbers[i] = fieldNumber;
        }
      }
      return this;
    }

    public Function<Output, Output> build(
      Schema<?> startingSchema
    ) {
      return (output) ->
        new BlackListOutput(
          output,
          blackListed,
          blackListed.getOrDefault(startingSchema.messageFullName(), EmptyArray)
        );
    }
  }
}
