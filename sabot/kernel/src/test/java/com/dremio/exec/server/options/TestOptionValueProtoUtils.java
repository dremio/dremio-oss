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
package com.dremio.exec.server.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.dremio.options.OptionValue;
import com.dremio.options.OptionValueProto;

/**
 * Tests for {@link OptionValueProtoUtils}
 */
public class TestOptionValueProtoUtils {

  @Test
  public void testBoolOptionToProto() {
    final OptionValue option = OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "test.option", true);
    final OptionValueProto optionProto = OptionValueProtoUtils.toOptionValueProto(option);
    assertTrue(verifyEquivalent(option, optionProto));
  }

  @Test
  public void testLongOptionToProto() {
    final OptionValue option = OptionValue.createLong(OptionValue.OptionType.SYSTEM, "test.option", 1234);
    final OptionValueProto optionProto = OptionValueProtoUtils.toOptionValueProto(option);
    assertTrue(verifyEquivalent(option, optionProto));
  }

  @Test
  public void testStringOptionToProto() {
    final OptionValue option = OptionValue.createString(OptionValue.OptionType.SYSTEM, "test.option", "test-option");
    final OptionValueProto optionProto = OptionValueProtoUtils.toOptionValueProto(option);
    assertTrue(verifyEquivalent(option, optionProto));
  }

  @Test
  public void testDoubleOptionToProto() {
    final OptionValue option = OptionValue.createDouble(OptionValue.OptionType.SYSTEM, "test.option", 1234.1234);
    final OptionValueProto optionProto = OptionValueProtoUtils.toOptionValueProto(option);
    assertTrue(verifyEquivalent(option, optionProto));
  }

  @Test
  public void testBoolOptionFromProto() {
    final OptionValueProto optionProto = OptionValueProto.newBuilder()
      .setName("test.option")
      .setBoolVal(true)
      .build();
    final OptionValue option = OptionValueProtoUtils.toOptionValue(optionProto);
    assertTrue(verifyEquivalent(option, optionProto));
  }

  @Test
  public void testLongOptionFromProto() {
    final OptionValueProto optionProto = OptionValueProto.newBuilder()
      .setName("test.option")
      .setNumVal(1234)
      .build();
    final OptionValue option = OptionValueProtoUtils.toOptionValue(optionProto);
    assertTrue(verifyEquivalent(option, optionProto));
  }

  @Test
  public void testStringOptionFromProto() {
    final OptionValueProto optionProto = OptionValueProto.newBuilder()
      .setName("test.option")
      .setStringVal("test-option")
      .build();
    final OptionValue option = OptionValueProtoUtils.toOptionValue(optionProto);
    assertTrue(verifyEquivalent(option, optionProto));
  }

  @Test
  public void testFloatOptionFromProto() {
    final OptionValueProto optionProto = OptionValueProto.newBuilder()
      .setName("test.option")
      .setFloatVal(1234.1234)
      .build();
    final OptionValue option = OptionValueProtoUtils.toOptionValue(optionProto);
    assertTrue(verifyEquivalent(option, optionProto));
  }

  private boolean verifyEquivalent(OptionValue optionValue, OptionValueProto optionValueProto) {
    checkName(optionValue, optionValueProto);

    switch(optionValue.getKind()) {
      case BOOLEAN:
        return optionValue.getBoolVal().equals(optionValueProto.getBoolVal());
      case LONG:
        return optionValue.getNumVal().equals(optionValueProto.getNumVal());
      case STRING:
        return optionValue.getStringVal().equals(optionValueProto.getStringVal());
      case DOUBLE:
        return optionValue.getFloatVal().equals(optionValueProto.getFloatVal());
      default:
        throw new AssertionError();
    }
  }

  private void checkName(OptionValue optionValue, OptionValueProto optionValueProto) {
    assertEquals(optionValue.getName(), optionValueProto.getName());
  }
}
