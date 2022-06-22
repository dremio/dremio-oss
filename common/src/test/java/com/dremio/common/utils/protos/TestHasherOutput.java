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

import org.junit.Assert;
import org.junit.Test;

import io.protostuff.ByteString;

/**
 * Tests HasherOutput
 */
public class TestHasherOutput {

  @Test
  public void testNoInput() {
    HasherOutput subject = new HasherOutput();

    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", result);
  }

  // Write Booleans

  @Test
  public void testBooleanField1InputFalse() {
    HasherOutput subject = new HasherOutput();
    subject.writeBool(1, false, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("957b88b12730e646e0f33d3618b77dfa579e8231e3c59c7104be7165611c8027", result);
  }

  @Test
  public void testBooleanField1InputTrue() {
    HasherOutput subject = new HasherOutput();
    subject.writeBool(1, true, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("a1cb20470d89874f33383802c72d3c27a0668ebffd81934705ab0cfcbf1a1e3a", result);
  }

  @Test
  public void testBooleanField2InputFalse() {
    HasherOutput subject = new HasherOutput();
    subject.writeBool(2, false, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("395c2f5598a1643a205154c6f4c46ce36895b28e6c35660a95e5c6fd5ef9aeab", result);
  }

  @Test
  public void testBooleanField2InputTrue() {
    HasherOutput subject = new HasherOutput();
    subject.writeBool(2, true, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("67c60c6612920fc8c68c55d63eadb34b0812235d7b2bf4f13f5692ed8f0cd856", result);
  }

  // Write Strings

  @Test
  public void testStringField1InputFalse() {
    HasherOutput subject = new HasherOutput();
    subject.writeString(1, "false", false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("6388c3273ad37b79708d37d3aa7eb6bfdde0d537702d5e391f34ee05e51e664f", result);
  }

  @Test
  public void testStringField1InputTrue() {
    HasherOutput subject = new HasherOutput();
    subject.writeString(1, "true", false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("ee4dfb1ebb10c467cee745bef15bf091496e6bd6e1e50a3f07688269396eb199", result);
  }

  @Test
  public void testStringField2InputFalse() {
    HasherOutput subject = new HasherOutput();
    subject.writeString(2, "false", false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("cadb330ef18bdd78b300904622c4c763c2d671da995ef7b13972f94ad254c96e", result);
  }


  @Test
  public void testStringField2InputTrue() {
    HasherOutput subject = new HasherOutput();
    subject.writeString(2, "true", false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("58ef1f87abe84b2bf93d9dcdda662c7405241ab4ca5eb5274c227c30300d917a", result);
  }

  // Write Int32

  @Test
  public void testInt32Field1InputOne() {
    HasherOutput subject = new HasherOutput();
    subject.writeInt32(1, 1, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("64ed86b909d6d0502b64b28db0ea1272ffb358e20e9b1d88b63ccb07fa900cf5", result);
  }

  @Test
  public void testInt32Field1InputTwo() {
    HasherOutput subject = new HasherOutput();
    subject.writeInt32(1, 2, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("34fb5c825de7ca4aea6e712f19d439c1da0c92c37b423936c5f618545ca4fa1f", result);
  }

  @Test
  public void testInt32Field2InputOne() {
    HasherOutput subject = new HasherOutput();
    subject.writeInt32(2, 1, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("7b2ed67587fcbc411fcb4b71b1cef1ef6cd9edf948148414cf5f0ab21362b9aa", result);
  }

  @Test
  public void testInt32Field2InputTwo() {
    HasherOutput subject = new HasherOutput();
    subject.writeInt32(2, 2, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("41d805e613efbe1acb36eaa0127a35da4a1faa1ac97985d078e50d1fd96055fd", result);
  }

  // Write Int64

  @Test
  public void testInt64Field1InputOne() {
    HasherOutput subject = new HasherOutput();
    subject.writeInt64(1, 1, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("7d450465ceb49083708a6970827f0e0b116ed285072a95b451e55f583f56da8d", result);
  }

  @Test
  public void testInt64Field1InputTwo() {
    HasherOutput subject = new HasherOutput();
    subject.writeInt64(1, 2, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("a7b5f185aee1c8e29b7e2c2bc39f2f83216282354e7a0753fa1263fa399e6c4a", result);
  }

  @Test
  public void testInt64Field2InputOne() {
    HasherOutput subject = new HasherOutput();
    subject.writeInt64(2, 1, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("363f012b74b9c88d828c809a568dc50627214866415aeb36ef6effbc3061741f", result);
  }

  @Test
  public void testInt64Field2InputTwo() {
    HasherOutput subject = new HasherOutput();
    subject.writeInt64(2, 2, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("a8dad088d9b94a2c8e44124ecc14093af1ec3b0b87b6ae5c9f181868ce7505fd", result);
  }

  // Write bytes

  @Test
  public void testBytesStringField1InputOne() {
    HasherOutput subject = new HasherOutput();
    subject.writeBytes(1, ByteString.bytesDefaultValue("1"), false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("d7c0db5a2a0b7eb9abf5c315453ababfd88818917d8672d8c8889558477d6102", result);
  }

  @Test
  public void testBytesStringField1InputTwo() {
    HasherOutput subject = new HasherOutput();
    subject.writeBytes(1, ByteString.bytesDefaultValue("2"), false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("59207f4cecc0f64dcbdf9e17b220ba9a00d4323709c095ce72a192707cd02b9c", result);
  }

  @Test
  public void testBytesStringField2InputOne() {
    HasherOutput subject = new HasherOutput();
    subject.writeBytes(2, ByteString.bytesDefaultValue("1"), false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("b83c558e83283cb9a723fd1cee5a4c5c934d3f1ceacb14a29642b50b8095ce34", result);
  }

  @Test
  public void testBytesStringField2InputTwo() {
    HasherOutput subject = new HasherOutput();
    subject.writeBytes(2, ByteString.bytesDefaultValue("2"), false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("b33d28bbd08cffe5a2d24d157ce92cc2ab1e538f27d5a5dd6312d9aae078582d", result);
  }

  // Write enum

  @Test
  public void testEnumField1InputOne() {
    HasherOutput subject = new HasherOutput();
    subject.writeEnum(1, 1, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("64ed86b909d6d0502b64b28db0ea1272ffb358e20e9b1d88b63ccb07fa900cf5", result);
  }

  @Test
  public void testEnumField1InputTwo() {
    HasherOutput subject = new HasherOutput();
    subject.writeEnum(1, 2, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("34fb5c825de7ca4aea6e712f19d439c1da0c92c37b423936c5f618545ca4fa1f", result);
  }

  @Test
  public void testEnumField2InputOne() {
    HasherOutput subject = new HasherOutput();
    subject.writeEnum(2, 1, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("7b2ed67587fcbc411fcb4b71b1cef1ef6cd9edf948148414cf5f0ab21362b9aa", result);
  }

  @Test
  public void testEnumField2InputTwo() {
    HasherOutput subject = new HasherOutput();
    subject.writeEnum(2, 2, false);
    String result = subject.getHasher().hash().toString();
    Assert.assertEquals("41d805e613efbe1acb36eaa0127a35da4a1faa1ac97985d078e50d1fd96055fd", result);
  }
}
