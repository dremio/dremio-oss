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

package com.dremio.service.reflection;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.refresh.RefreshHandler;
import com.google.common.collect.ImmutableMap;

/**
 * tests for {@link RefreshHandler}
 */
public class TestWriterOptionManager {

  @Test public void testValidateAndPluckNames(){
    List<ReflectionField> fields = Arrays.asList(
      new ReflectionField()
        .setName("my_name")
    );
    Map<String, String> knownFields = ImmutableMap.of("my_name", "my_name");

    WriterOptionManager subject = WriterOptionManager.Instance;

    List<String> actual = subject.validateAndPluckNames(fields, knownFields);

    Assert.assertEquals(
      Arrays.asList(
        "my_name"
      ),
      actual
    );
  }

  @Test public void testValidateAndPluckNameMissing(){
    List<ReflectionField> fields = Arrays.asList(
      new ReflectionField()
        .setName("Unknown Field")
    );
    Map<String, String> knownFields = ImmutableMap.of("my field", "my field");

    WriterOptionManager subject = WriterOptionManager.Instance;


    try {
      subject.validateAndPluckNames(fields, knownFields);
      Assert.fail("Expected exception for unknown field");
    } catch (UserException userException) {
      Assert.assertEquals(
        "Unable to find field ReflectionField{name=Unknown Field}.",
        userException.getMessage()
      );
    }
  }
}
