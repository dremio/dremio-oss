/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.server;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.spaces.Space;
import com.dremio.dac.service.errors.ClientErrorException;

/**
 * Test input validation
 */
public class TestInputValidation {
  @Test
  public void testSpaceValidation() {
    try {
      new InputValidation().validate(new Space(null, "\"", null, null, null, 0, null));
      Assert.fail("expected exception");
    } catch (ClientErrorException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("Space name can not contain periods or double quotes"));
    }

    try {
      new InputValidation().validate(new Space(null, ".", null, null, null, 0, null));
      Assert.fail("expected exception");
    } catch (ClientErrorException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("Space name can not contain periods or double quotes"));
    }

    try {
      new InputValidation().validate(new Space(null, "longer \" test", null, null, null, 0, null));
      Assert.fail("expected exception");
    } catch (ClientErrorException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("Space name can not contain periods or double quotes"));
    }
  }

  @Test
  public void testSourceValidation() {
    try {
      SourceUI config = new SourceUI();
      config.setName("\"");
      new InputValidation().validate(config);
      Assert.fail("expected exception");
    } catch (ClientErrorException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("Source name can not contain periods or double quotes"));
    }

    try {
      SourceUI config = new SourceUI();
      config.setName(".");
      new InputValidation().validate(config);
      Assert.fail("expected exception");
    } catch (ClientErrorException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("Source name can not contain periods or double quotes"));
    }

    try {
      SourceUI config = new SourceUI();
      config.setName("longer \" test");
      new InputValidation().validate(config);
      Assert.fail("expected exception");
    } catch (ClientErrorException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("Source name can not contain periods or double quotes"));
    }
  }
}
