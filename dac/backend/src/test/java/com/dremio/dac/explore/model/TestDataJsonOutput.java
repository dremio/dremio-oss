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
package com.dremio.dac.explore.model;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.DatabindContext;

/**
 * Unit tests for {@link DataJsonOutput}
 */
@RunWith(Parameterized.class)
public class TestDataJsonOutput {
    @Parameterized.Parameters(name = "Test isNumberAsString. Attribute value = {0}, expected value = {1}")
    public static Collection<Object[]> data() {
      return asList(new Object[][]{
        { null, false },
        { false, false },
        { new Object(), false },
        { "a string", false },
        { true, true },
      });
    }

    private final DatabindContext context;
    private final Object inputValue;
    private final boolean expectedValue;

    public TestDataJsonOutput(final Object inputValue, final boolean expectedValue) {
       this.context = Mockito.mock(DatabindContext.class);
       this.inputValue = inputValue;
       this.expectedValue = expectedValue;
    }

    @Test
    public void test() {
      Mockito.when(context.getAttribute(DataJsonOutput.DREMIO_JOB_DATA_NUMBERS_AS_STRINGS_ATTRIBUTE)).thenReturn(this.inputValue);
      assertEquals(this.expectedValue, DataJsonOutput.isNumberAsString(context));
    }
}
