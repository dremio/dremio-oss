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
package com.dremio.exec.vector.accessor;

import org.apache.arrow.vector.ValueVector;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.vector.accessor.GenericAccessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Ignore("getMetadata method was removed, cannot mock")
public class GenericAccessorTest {

  public static final Object NON_NULL_VALUE = "Non-null value";

  private GenericAccessor genericAccessor;
  private ValueVector valueVector;
  private UserBitShared.SerializedField metadata;

  @Before
  public void setUp() throws Exception {
    valueVector = mock(ValueVector.class);
    when(valueVector.getObject(anyInt())).thenAnswer(new Answer<Object>() {

      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        Object[] args = invocationOnMock.getArguments();
        Integer index = (Integer) args[0];
        if(index == 0) {
          return NON_NULL_VALUE;
        }
        if(index == 1) {
          return null;
        }
        throw new IndexOutOfBoundsException("Index out of bounds");
      }
    });
    when(valueVector.isNull(anyInt())).thenAnswer(new Answer<Object>() {

      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        Object[] args = invocationOnMock.getArguments();
        Integer index = (Integer) args[0];
        if(index == 0) {
          return false;
        }
        return true;
      }
    });

    metadata = UserBitShared.SerializedField.getDefaultInstance();
    when(TypeHelper.getMetadata(valueVector)).thenReturn(metadata);

    genericAccessor = new GenericAccessor(valueVector);
  }

  @Test
  public void testIsNull() throws Exception {
    assertFalse(genericAccessor.isNull(0));
    assertTrue(genericAccessor.isNull(1));
  }

  @Test
  public void testGetObject() throws Exception {
    assertEquals(NON_NULL_VALUE, genericAccessor.getObject(0));
    assertNull(genericAccessor.getObject(1));
  }

  @Test(expected=IndexOutOfBoundsException.class)
  public void testGetObject_indexOutOfBounds() throws Exception {
    genericAccessor.getObject(2);
  }

  @Test
  public void testGetType() throws Exception {
    assertEquals(UserBitShared.SerializedField.getDefaultInstance().getMajorType(), genericAccessor.getType());
  }
}
