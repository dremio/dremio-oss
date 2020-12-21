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
package com.dremio.exec.store.easy.arrow;

import static com.dremio.exec.store.easy.arrow.ArrowFileReader.toBean;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;

import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.exec.proto.beans.SerializedField;

public class TestArrowFileMetadataValidator {

  @Test
  public void testContainsUnionWithNullField() {
    // assert
    assertFalse(ArrowFileMetadataValidator.containsUnion(null));
  }

  @Test
  public void testContainsUnionWithSingleUnionField() {
    // arrange
    final SerializedField unionField = new SerializedField().setMajorType(
      toBean(Types.required(TypeProtos.MinorType.UNION)));

    // assert
    assertTrue(ArrowFileMetadataValidator.containsUnion(unionField));
  }

  @Test
  public void testContainsUnionWithSingleNonUnionField() {
    // arrange
    final SerializedField nonUnionField = new SerializedField().setMajorType(
      toBean(Types.required(TypeProtos.MinorType.BIGINT)));

    // assert
    assertFalse(ArrowFileMetadataValidator.containsUnion(nonUnionField));
  }

  @Test
  public void testContainsUnionWithParentUnionField() {
    // arrange
    final SerializedField parentUnionField = new SerializedField().setMajorType(
      toBean(Types.required(TypeProtos.MinorType.UNION)));
    final SerializedField childNonUnionField = new SerializedField().setMajorType(
      toBean(Types.required(TypeProtos.MinorType.INT)));
    parentUnionField.setChildList(Collections.singletonList(childNonUnionField));

    // assert
    assertTrue(ArrowFileMetadataValidator.containsUnion(parentUnionField));
  }

  @Test
  public void testContainsUnionWithChildUnionField() {
    // arrange
    final SerializedField parentNonUnionField = new SerializedField().setMajorType(
      toBean(Types.required(TypeProtos.MinorType.VARCHAR)));
    final SerializedField childUnionField = new SerializedField().setMajorType(
      toBean(Types.required(TypeProtos.MinorType.UNION)));
    parentNonUnionField.setChildList(Collections.singletonList(childUnionField));

    // assert
    assertTrue(ArrowFileMetadataValidator.containsUnion(parentNonUnionField));
  }
}
