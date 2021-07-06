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

package com.dremio.plugins.azure.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests for {@link AzureAsyncHttpClientUtils}
 */
public class AzureAsyncHttpClientUtilsTest {

    @Test
    public void testEncodeUrlWithSpaces() {
        assertEquals("dir%20with%20spaces%2Fexample.parquet", AzureAsyncHttpClientUtils.encodeUrl("dir with spaces/example.parquet"));
    }
}
