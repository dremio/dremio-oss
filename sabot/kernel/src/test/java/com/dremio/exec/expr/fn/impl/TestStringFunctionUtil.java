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
package com.dremio.exec.expr.fn.impl;

import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;

/**
 * Unit tests for StringFunctionUtil
 */
public class TestStringFunctionUtil {
  protected BufferAllocator allocator;

  @Before
  public void setup(){
    this.allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void close() throws Exception {
    AutoCloseables.close(allocator);
  }

  // Check if the returned ArrowBuf has the same contents as the expected string
  private void assertSameAsExpected(String expected, ArrowBuf buf, int bufLen) {
    assertEquals(expected.length(), bufLen);
    byte[] destContents = new byte[bufLen];
    buf.getBytes(0, destContents);
    byte[] expectedContents = expected.getBytes();
    for (int i = 0; i < bufLen; i++) {
      assertEquals(String.format("mismatch at position %d", i), expectedContents[i], destContents[i]);
    }
  }

  // Copy only the UTF8 parts of 'in', expecting to receive 'expected' as a result
  private void testCopyUtf8Helper(byte[] in, String expected) throws Exception {
    ByteBuf src = allocator.buffer(in.length + 1);
    ArrowBuf dest = allocator.buffer(in.length);
    src.writeByte(0x20);  // one extra byte, just to test startIdx != 0
    src.writeBytes(in);

    int destLen = StringFunctionUtil.copyUtf8(src, src.readerIndex() + 1, src.writerIndex(), dest);
    assertSameAsExpected(expected, dest, destLen);
    src.release();
    dest.release();
  }

  @Test
  public void testCopyUtf8() throws Exception {
    testCopyUtf8Helper(new byte[] {'g', 'o', 'o', 'd', 'v', 'a', 'l'}, "goodval");
    testCopyUtf8Helper(new byte[] {'b', 'a', 'd', (byte)0xff, 'v', 'a', 'l'}, "badval");
    testCopyUtf8Helper(new byte[] {(byte)0xf9, 'g', 'o', 'o', 'd', ' ', 'p', 'a', 'r', 't'}, "good part");
    testCopyUtf8Helper(new byte[] {'t', 'h', 'i', 's', ' ', 'i', 's', ' ', 'o', 'k', (byte)0xfe}, "this is ok");
    testCopyUtf8Helper(new byte[] {'f', 'a', 'k', 'e', ' ', (byte) 0xC0, '2', 'B', ' ', 's', 'e', 'q', }, "fake 2B seq");
  }

  // Replace the non-UTF8 parts of 'in' with 'replace', expecting to receive 'expected' as a result
  private void testReplaceUtf8Helper(byte[] in, byte replace, String expected) throws Exception {
    ByteBuf src = allocator.buffer(in.length + 1);
    ArrowBuf dest = allocator.buffer(in.length);
    src.writeByte(0x20);  // one extra byte, just to test startIdx != 0
    src.writeBytes(in);

    int destLen = StringFunctionUtil.copyReplaceUtf8(src, src.readerIndex() + 1, src.writerIndex(), dest, replace);
    assertSameAsExpected(expected, dest, destLen);
    src.release();
    dest.release();
  }

  @Test
  public void testCopyReplaceUtf8() throws Exception {
    testReplaceUtf8Helper(new byte[] {'g', 'o', 'o', 'd', 'v', 'a', 'l'},  (byte)'?', "goodval");
    testReplaceUtf8Helper(new byte[] {'b', 'a', 'd', (byte)0xff, 'v', 'a', 'l'}, (byte)'?', "bad?val");
    testReplaceUtf8Helper(new byte[] {(byte)0xf9, 'g', 'o', 'o', 'd', ' ', 'p', 'a', 'r', 't'}, (byte)'X', "Xgood part");
    testReplaceUtf8Helper(new byte[] {'t', 'h', 'i', 's', ' ', 'i', 's', ' ', 'o', 'k', (byte)0xfe}, (byte)'|', "this is ok|");
    testReplaceUtf8Helper(new byte[] {'f', 'a', 'k', 'e', ' ', (byte) 0xC0, '2', 'B', ' ', 's', 'e', 'q', }, (byte)'?', "fake ?2B seq");
  }

  private void testIsUtf8Helper(byte[] in, boolean expected) {
    ByteBuf src = allocator.buffer(in.length + 1);
    src.writeByte(0x20);  // one extra byte, just to test startIdx != 0
    src.writeBytes(in);

    assertEquals(expected, GuavaUtf8.isUtf8(src, src.readerIndex() + 1, src.writerIndex()));
    src.release();
  }

  @Test
  public void testIsUtf8() throws Exception {
    testIsUtf8Helper(new byte[] {'g', 'o', 'o', 'd', 'v', 'a', 'l'},  true);
    testIsUtf8Helper(new byte[] {'b', 'a', 'd', (byte)0xff, 'v', 'a', 'l'}, false);
    testIsUtf8Helper(new byte[] {(byte)0xf9, 'x', 'y', 'z'}, false);
    testIsUtf8Helper(new byte[] {'x', 'y', 'z', (byte)0xf9}, false);
  }
}
