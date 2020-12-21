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
package com.dremio.sabot.rpc.user;

import static com.dremio.sabot.rpc.user.BaseBackwardsCompatibilityHandler.fieldBuffersCount;
import static com.dremio.sabot.rpc.user.DrillBackwardsCompatibilityHandler.convertBitsToBytes;
import static com.dremio.sabot.rpc.user.DrillBackwardsCompatibilityHandler.padValues;
import static com.dremio.sabot.rpc.user.DrillBackwardsCompatibilityHandler.patchDecimal;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalHelper;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedWidthVectorHelper;
import org.apache.arrow.vector.UInt1Vector;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared.SerializedField;
import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;

/**
 * Test valid behavior of backward compatibility.
 */
public class TestBackwardsCompatibilityHandler extends DremioTest {

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  private BufferAllocator allocator;

  @Before
  public void before() {
    this.allocator = allocatorRule.newAllocator("test-backwards-compatibility-handler", 0, Long.MAX_VALUE);
  }

  @After
  public void after() {
    this.allocator.close();
  }

  private SerializedField.Builder[] fields(int... size) {
    SerializedField.Builder[] result = new SerializedField.Builder[size.length];
    for (int i = 0; i < size.length; i++) {
      result[i] = SerializedField.newBuilder()
          .setBufferLength(size[i]).setValueCount(1);
    }
    return result;
  }

  private ByteBuf[] bufs(int... size) {
    ByteBuf[] result = new ByteBuf[size.length];
    for (int i = 0; i < size.length; i++) {
      result[i] = buf(size[i]);
    }
    return result;
  }

  private ByteBuf buf(int size) {
    return NettyArrowBuf.unwrapBuffer(allocator.buffer(size).writerIndex(size));
  }

  @Test
  public void testFieldBuffersCount() {
    ByteBuf[] buffers = bufs(1, 8, 5, 1, 8, 35, 1, 8, 0);
    SerializedField.Builder[] fields = fields(14, 44, 9);
    testBuffers(buffers, fields);
  }

  @Test
  public void testFieldBuffersCountFirstChild() {
    ByteBuf[] buffers = bufs(1, 8, 5);
    SerializedField.Builder[] fields = fields(1, 13);
    testBuffers(buffers, fields);
  }

  @Test
  public void testFieldBuffersCountFirstChildLC() {
    ByteBuf[] buffers = bufs(8, 5);
    SerializedField.Builder[] fields = fields(8, 5);
    testBuffers(buffers, fields);
  }

  @Test
  public void testFieldBuffersCountLastChild() {
    ByteBuf[] buffers = bufs(1, 8);
    SerializedField.Builder[] fields = fields(1, 8);
    testBuffers(buffers, fields);
  }

  @Test
  public void testFieldBuffersCountLastChildLC() {
    ByteBuf[] buffers = bufs(8);
    SerializedField.Builder[] fields = fields(8, 0);
    testBuffers(buffers, fields);
  }

  private void testBuffers(ByteBuf[] buffers, SerializedField.Builder[] fields) {
    int i = 0;
    for (SerializedField.Builder field : fields) {
      i += fieldBuffersCount(field, buffers, i, buffers.length);
    }
    while (i < buffers.length && buffers[i].readableBytes() == 0) {
      ++ i;
    }
    assertEquals(buffers.length, i);
    for (ByteBuf byteBuf : buffers) {
      byteBuf.release();
    }
  }

  @Test
  public void testConvertBitsToBytes() {
    try (BitVector bits = new BitVector("$bits$", allocator);
        UInt1Vector bytes = new UInt1Vector("$bits$", allocator);
    ) {

      int count = 100;
      for (int i = 0; i < count ; i++) {
        bits.setSafe(i, i % 2);
      }
      bits.setValueCount(count);

      ArrowBuf oldBuf = bits.getDataBuffer();
      oldBuf.retain();
      SerializedField.Builder fieldBuilder = TypeHelper.getMetadataBuilder(bits);
      ArrowBuf newBuf = convertBitsToBytes(allocator, fieldBuilder, NettyArrowBuf.unwrapBuffer(oldBuf)).arrowBuf();
      bytes.setValueCount(count);
      SerializedField.Builder newfieldBuilder = TypeHelper.getMetadataBuilder(bytes);
      TypeHelper.loadData(bytes, newfieldBuilder.build(), newBuf);
      for (int i = 0; i < count ; i++) {
        assertEquals(i % 2, bytes.get(i));
      }
      newBuf.release();
    }
  }

  @Test
  public void testPatchDecimal() {
    DecimalVector decimalVector = new DecimalVector("decimal", allocator, 38, 9);
    decimalVector.allocateNew(8);

    BigDecimal decimal1 = new BigDecimal("123456789.000000000");
    BigDecimal decimal2 = new BigDecimal("11.123456789");
    BigDecimal decimal3 = new BigDecimal("1.000000000");
    BigDecimal decimal4 = new BigDecimal("0.111111111");
    BigDecimal decimal5 = new BigDecimal("-987654321.123456789"); BigDecimal decimal6 = new BigDecimal("-222222222222.222222222");
    BigDecimal decimal7 = new BigDecimal("-7777777777777.666666667");
    BigDecimal decimal8 = new BigDecimal("1212121212.343434343");

    decimalVector.set(0, decimal1);
    decimalVector.set(1, decimal2);
    decimalVector.set(2, decimal3);
    decimalVector.set(3, decimal4);
    decimalVector.set(4, decimal5);
    decimalVector.set(5, decimal6);
    decimalVector.set(6, decimal7);
    decimalVector.set(7, decimal8);

    decimalVector.setValueCount(8);

    assertEquals(8, decimalVector.getValueCount());
    assertEquals(decimal1, decimalVector.getObject(0));
    assertEquals(decimal2, decimalVector.getObject(1));
    assertEquals(decimal3, decimalVector.getObject(2));
    assertEquals(decimal4, decimalVector.getObject(3));
    assertEquals(decimal5, decimalVector.getObject(4));
    assertEquals(decimal6, decimalVector.getObject(5));
    assertEquals(decimal7, decimalVector.getObject(6));
    assertEquals(decimal8, decimalVector.getObject(7));

    FixedWidthVectorHelper vectorHelper = new FixedWidthVectorHelper(decimalVector);
    SerializedField.Builder decimalField = vectorHelper.getMetadataBuilder();
    SerializedField.Builder childDecimalField = decimalField.getChildBuilderList().get(1);
    ByteBuf newBuffer = patchDecimal(allocator, NettyArrowBuf.unwrapBuffer(decimalVector.getDataBuffer()), decimalField,
      childDecimalField);

    int startIndex = 0;
    BigDecimal bd = DecimalHelper.getBigDecimalFromSparse(newBuffer, startIndex, DrillBackwardsCompatibilityHandler.NUMBER_DECIMAL_DIGITS, decimalVector.getScale());
    assertEquals(bd, decimal1);
    startIndex += DecimalVector.TYPE_WIDTH + 8;

    bd = DecimalHelper.getBigDecimalFromSparse(newBuffer, startIndex, DrillBackwardsCompatibilityHandler.NUMBER_DECIMAL_DIGITS, decimalVector.getScale());
    assertEquals(bd, decimal2);
    startIndex += DecimalVector.TYPE_WIDTH + 8;

    bd = DecimalHelper.getBigDecimalFromSparse(newBuffer, startIndex, DrillBackwardsCompatibilityHandler.NUMBER_DECIMAL_DIGITS, decimalVector.getScale());
    assertEquals(bd, decimal3);
    startIndex += DecimalVector.TYPE_WIDTH + 8;

    bd = DecimalHelper.getBigDecimalFromSparse(newBuffer, startIndex, DrillBackwardsCompatibilityHandler.NUMBER_DECIMAL_DIGITS, decimalVector.getScale());
    assertEquals(bd, decimal4);
    startIndex += DecimalVector.TYPE_WIDTH + 8;

    bd = DecimalHelper.getBigDecimalFromSparse(newBuffer, startIndex, DrillBackwardsCompatibilityHandler.NUMBER_DECIMAL_DIGITS, decimalVector.getScale());
    assertEquals(bd, decimal5);
    startIndex += DecimalVector.TYPE_WIDTH + 8;

    bd = DecimalHelper.getBigDecimalFromSparse(newBuffer, startIndex, DrillBackwardsCompatibilityHandler.NUMBER_DECIMAL_DIGITS, decimalVector.getScale());
    assertEquals(bd, decimal6);
    startIndex += DecimalVector.TYPE_WIDTH + 8;

    bd = DecimalHelper.getBigDecimalFromSparse(newBuffer, startIndex, DrillBackwardsCompatibilityHandler.NUMBER_DECIMAL_DIGITS, decimalVector.getScale());
    assertEquals(bd, decimal7);
    startIndex += DecimalVector.TYPE_WIDTH + 8;

    bd = DecimalHelper.getBigDecimalFromSparse(newBuffer, startIndex, DrillBackwardsCompatibilityHandler.NUMBER_DECIMAL_DIGITS, decimalVector.getScale());
    assertEquals(bd, decimal8);

    final ArrowBuf validityBuffer = decimalVector.getValidityBuffer();
    validityBuffer.release();
    newBuffer.release();
  }

  @Test
  public void testPadValues() {
    int originalTypeByteWidth = 8;
    int targetTypeByteWidth = 12;
    try (
        UInt1Vector bytes = new UInt1Vector("$bits$", allocator);
    ) {

      int count = 100;
      for (int i = 0; i < count * 8 ; i++) {
        bytes.setSafe(i, i % 8);
      }
      bytes.setValueCount(count * 8);

      ArrowBuf oldBuf = bytes.getDataBuffer();
      oldBuf.retain();
      SerializedField.Builder fieldBuilder = TypeHelper.getMetadataBuilder(bytes);
      ArrowBuf newBuf = padValues(allocator, fieldBuilder, NettyArrowBuf.unwrapBuffer(oldBuf), originalTypeByteWidth,
        targetTypeByteWidth).arrowBuf();
      bytes.setValueCount(count * 12);
      SerializedField.Builder newfieldBuilder = TypeHelper.getMetadataBuilder(bytes);
      // load data in newBuf into bytes, all the validity will be set to one
      TypeHelper.loadData(bytes, newfieldBuilder.build(), newBuf);
      for (int i = 0; i < count ; i++) {
        for (int byteIndex = 0; byteIndex < 8 ; byteIndex++) {
          assertEquals((i * 8 + byteIndex) % 8, bytes.get(i * 12 + byteIndex));
        }
      }
      newBuf.release();
    }
  }
}
