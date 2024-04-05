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
package com.dremio.common.utils;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.Schema;
import java.io.IOException;
import org.junit.Test;

/** Test class for {@code ProtostuffUtil} helper methods */
public class TestProtostuffUtils {
  private static final class Foo {
    private double bar;
  }

  private static final Schema<Foo> SCHEMA =
      new Schema<TestProtostuffUtils.Foo>() {

        @Override
        public String getFieldName(int number) {
          switch (number) {
            case 1:
              return "bar";
            default:
              throw new AssertionError(format("Unknown field id %d", number));
          }
        }

        @Override
        public int getFieldNumber(String name) {
          switch (name) {
            case "bar":
              return 1;
            default:
              throw new AssertionError(format("Unknown field name %s", name));
          }
        }

        @Override
        public boolean isInitialized(Foo message) {
          return true;
        }

        @Override
        public Foo newMessage() {
          return new Foo();
        }

        @Override
        public String messageName() {
          return "Foo";
        }

        @Override
        public String messageFullName() {
          return Foo.class.getName();
        }

        @Override
        public Class<? super Foo> typeClass() {
          return Foo.class;
        }

        @Override
        public void mergeFrom(Input input, Foo message) throws IOException {
          for (int number = input.readFieldNumber(this); ; number = input.readFieldNumber(this)) {
            switch (number) {
              case 0:
                return;
              case 1:
                message.bar = input.readDouble();
                break;
              default:
                input.handleUnknownField(number, this);
            }
          }
        }

        @Override
        public void writeTo(Output output, Foo message) throws IOException {
          output.writeDouble(1, message.bar, false);
        }
      };

  @Test
  public void testDoubleDeserialization() throws IOException {
    Foo foo = ProtostuffUtil.fromJSON("{ \"bar\": 42.0 }", SCHEMA, false);
    assertEquals(42.0d, foo.bar, 0.0001);
  }

  @Test
  public void testNanDeserialization() throws IOException {
    Foo foo = ProtostuffUtil.fromJSON("{ \"bar\": NaN }", SCHEMA, false);
    assertEquals(Double.NaN, foo.bar, 0);
  }

  @Test
  public void testDoubleSerialization() throws IOException {
    Foo foo = new Foo();
    foo.bar = 42.0d;

    assertThat(ProtostuffUtil.toJSON(foo, SCHEMA, false)).contains("42");
  }

  @Test
  public void testNanSerialization() throws IOException {
    Foo foo = new Foo();
    foo.bar = Double.NaN;

    assertThat(ProtostuffUtil.toJSON(foo, SCHEMA, false)).contains("NaN");
    assertThat(ProtostuffUtil.toJSON(foo, SCHEMA, false)).doesNotContain("\"NaN\"");
  }

  @Test
  public void testNanSerDe() throws IOException {
    Foo foo = new Foo();
    foo.bar = Double.NaN;

    assertEquals(
        Double.NaN,
        ProtostuffUtil.fromJSON(ProtostuffUtil.toJSON(foo, SCHEMA, false), SCHEMA, false).bar,
        0);
  }
}
