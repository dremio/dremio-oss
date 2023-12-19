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

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import io.protostuff.ByteString;
import io.protostuff.Output;
import io.protostuff.Schema;


public class TestBlackListOutput {

    @Test
    public void writeInt32() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeInt32(1, 9, false);
      subject.writeInt32(2, 8, false);

      Mockito.verify(delegate).writeInt32(1,9, false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeUInt32() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeUInt32(1, 9, false);
      subject.writeUInt32(2, 8, false);

      Mockito.verify(delegate).writeUInt32(1,9, false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeSInt32() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeSInt32(1, 9, false);
      subject.writeSInt32(2, 8, false);

      Mockito.verify(delegate).writeSInt32(1,9, false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeFixed32() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeFixed32(1, 9, false);
      subject.writeFixed32(2, 8, false);

      Mockito.verify(delegate).writeFixed32(1,9, false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeSFixed32() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeSFixed32(1, 9, false);
      subject.writeSFixed32(2, 8, false);

      Mockito.verify(delegate).writeSFixed32(1,9, false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeInt64() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeInt64(1, 9, false);
      subject.writeInt64(2, 8, false);

      Mockito.verify(delegate).writeInt64(1,9, false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeUInt64() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeUInt64(1, 9, false);
      subject.writeUInt64(2, 8, false);

      Mockito.verify(delegate).writeUInt64(1,9, false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeSInt64() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeSInt64(1, 9, false);
      subject.writeSInt64(2, 8, false);

      Mockito.verify(delegate).writeSInt64(1,9, false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeFixed64() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeFixed64(1, 9, false);
      subject.writeFixed64(2, 8, false);

      Mockito.verify(delegate).writeFixed64(1,9, false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeSFixed64() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeSFixed64(1, 9, false);
      subject.writeSFixed64(2, 8, false);

      Mockito.verify(delegate).writeSFixed64(1,9, false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeFloat() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeFloat(1, 9, false);
      subject.writeFloat(2, 8, false);

      Mockito.verify(delegate).writeFloat(1,9, false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeDouble() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeDouble(1, 9, false);
      subject.writeDouble(2, 8, false);

      Mockito.verify(delegate).writeDouble(1,9, false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeBool() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeBool(1, true, false);
      subject.writeBool(2, false, false);

      Mockito.verify(delegate).writeBool(1,true, false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeEnum() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeEnum(1, 1, false);
      subject.writeEnum(2, 2, false);

      Mockito.verify(delegate).writeEnum(1,1, false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeString() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeString(1, "1", false);
      subject.writeString(2, "2", false);

      Mockito.verify(delegate).writeString(1,"1", false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeByteString() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeBytes(1, ByteString.bytesDefaultValue("1"), false);
      subject.writeBytes(2, ByteString.bytesDefaultValue("2"), false);

      Mockito.verify(delegate).writeBytes(1,ByteString.bytesDefaultValue("1"), false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

  @Test
  public void writeByteBuffer() throws IOException {
    Output delegate = Mockito.mock(Output.class);
    BlackListOutput subject =
      new BlackListOutput(
        delegate,
        Collections.emptyMap(),
        new int[]{2}
      );

    subject.writeBytes(1, ByteBuffer.wrap(new byte[]{1}), false);
    subject.writeBytes(2, ByteBuffer.wrap(new byte[]{2}), false);

    Mockito.verify(delegate).writeBytes(1, ByteBuffer.wrap(new byte[]{1}), false);
    Mockito.verifyNoMoreInteractions(delegate);
  }

    @Test
    public void writeByteArray() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeByteArray(1, new byte[]{1}, false);
      subject.writeByteArray(2, new byte[]{2}, false);

      Mockito.verify(delegate).writeByteArray(1, new byte[]{1}, false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeByteRange() throws IOException {
      Output delegate = Mockito.mock(Output.class);
      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          Collections.emptyMap(),
          new int[]{2}
        );

      subject.writeByteRange(false,1, new byte[]{1}, 0, 1, false);
      subject.writeByteRange(false,2, new byte[]{2}, 0,1,false);

      Mockito.verify(delegate).writeByteRange(false,1, new byte[]{1}, 0, 1,false);
      Mockito.verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeObject() throws IOException {

      Output delegate = Mockito.mock(Output.class);
      Object object = new Object();
      String schemaName = "Hello Schema";
      String shortName = "Hello";
      Schema<Object> schema = Mockito.mock(Schema.class);
      int[] subSet = new int[]{4};

      BlackListOutput subject =
        new BlackListOutput(
          delegate,
          ImmutableMap.of(
            schemaName, subSet
          ),
          new int[]{2}
        );
      when(schema.messageFullName()).thenReturn(schemaName);
      when(schema.messageName()).thenReturn(shortName);

      Mockito.doAnswer((invocation) -> {
        Assert.assertEquals(subSet, subject.currentBlackList);
        return null;
      }).when(schema).writeTo(delegate, object);


      subject.writeObject(1, object, schema, false);

      Mockito.verify(schema).messageFullName();
      Mockito.verify(delegate).writeString(-1, schemaName, false);
      Mockito.verify(schema).writeTo(subject, object);
      Mockito.verify(delegate).writeString(-1, shortName, false);
      Mockito.verify(schema).messageName();

      Mockito.verifyNoMoreInteractions(schema);
    }

}
