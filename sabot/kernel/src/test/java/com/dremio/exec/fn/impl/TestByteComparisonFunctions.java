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
package com.dremio.exec.fn.impl;

import static org.junit.Assert.assertTrue;

import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.vector.ValueHolderHelper;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.ExecTest;

public class TestByteComparisonFunctions extends ExecTest {

  private static VarCharHolder hello;
  private static VarCharHolder goodbye;
  private static VarCharHolder helloLong;
  private static VarCharHolder goodbyeLong;

  @Before
  public void setup() {
    hello = ValueHolderHelper.getVarCharHolder(allocator, "hello");
    goodbye = ValueHolderHelper.getVarCharHolder(allocator, "goodbye");
    helloLong = ValueHolderHelper.getVarCharHolder(allocator, "hellomyfriend");
    goodbyeLong = ValueHolderHelper.getVarCharHolder(allocator, "goodbyemyenemy");
  }

  @After
  public void teardown() {
    hello.buffer.close();
    helloLong.buffer.close();
    goodbye.buffer.close();
    goodbyeLong.buffer.close();
  }

  @Test
  public void testAfter() {
    final VarCharHolder left = hello;
    final VarCharHolder right = goodbye;
    assertTrue(ByteFunctionHelpers.compare(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 1);
  }

  @Test
  public void testBefore() {
    final VarCharHolder left = goodbye;
    final VarCharHolder right = hello;
    assertTrue(ByteFunctionHelpers.compare(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == -1);
  }

  @Test
  public void testEqualCompare() {
    final VarCharHolder left = hello;
    final VarCharHolder right = hello;
    assertTrue(ByteFunctionHelpers.compare(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 0);
  }

  @Test
  public void testEqual() {
    final VarCharHolder left = hello;
    final VarCharHolder right = hello;
    assertTrue(ByteFunctionHelpers.equal(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 1);
  }

  @Test
  public void testNotEqual() {
    final VarCharHolder left = hello;
    final VarCharHolder right = goodbye;
    assertTrue(ByteFunctionHelpers.equal(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 0);
  }

  @Test
  public void testAfterLong() {
    final VarCharHolder left = helloLong;
    final VarCharHolder right = goodbyeLong;
    assertTrue(ByteFunctionHelpers.compare(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 1);
  }

  @Test
  public void testBeforeLong() {
    final VarCharHolder left = goodbyeLong;
    final VarCharHolder right = helloLong;
    assertTrue(ByteFunctionHelpers.compare(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == -1);
  }

  @Test
  public void testEqualCompareLong() {
    final VarCharHolder left = helloLong;
    final VarCharHolder right = helloLong;
    assertTrue(ByteFunctionHelpers.compare(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 0);
  }

  @Test
  public void testEqualLong() {
    final VarCharHolder left = helloLong;
    final VarCharHolder right = helloLong;
    assertTrue(ByteFunctionHelpers.equal(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 1);
  }

  @Test
  public void testNotEqualLong() {
    final VarCharHolder left = helloLong;
    final VarCharHolder right = goodbyeLong;
    assertTrue(ByteFunctionHelpers.equal(left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 0);
  }
}
