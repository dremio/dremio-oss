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
import com.dremio.file.FileName;

/**
 * Test input validation
 */
public class TestInputValidation {
  @Test
  public void testSpaceValidation() {
    Assert.assertTrue(didThrowValidationError(new Space(null, "\"", null, null, null, 0, null), "Space name can not contain periods or double quotes"));
    Assert.assertTrue(didThrowValidationError(new Space(null, ".", null, null, null, 0, null), "Space name can not contain periods or double quotes"));
    Assert.assertTrue(didThrowValidationError(new Space(null, "longer \" test", null, null, null, 0, null), "Space name can not contain periods or double quotes"));
  }

  @Test
  public void testSourceValidation() {
    SourceUI config = new SourceUI();
    config.setName("\"");
    Assert.assertTrue(didThrowValidationError(config, "Source name can not contain periods or double quotes"));

    config.setName(".");
    Assert.assertTrue(didThrowValidationError(config, "Source name can not contain periods or double quotes"));

    config.setName("longer \" test");
    Assert.assertTrue(didThrowValidationError(config, "Source name can not contain periods or double quotes"));
  }

  @Test
  public void testGeneralValidation() {
    Assert.assertTrue(didThrowValidationError(new FileName("afaf:dadad"), "File name cannot contain a colon, forward slash, at sign, or open curly bracket."));
    Assert.assertTrue(didThrowValidationError(new FileName(":"), "File name cannot contain a colon, forward slash, at sign, or open curly bracket."));
    Assert.assertTrue(didThrowValidationError(new FileName("/"), "File name cannot contain a colon, forward slash, at sign, or open curly bracket."));
    Assert.assertTrue(didThrowValidationError(new FileName("ada/adadad"), "File name cannot contain a colon, forward slash, at sign, or open curly bracket."));
    Assert.assertTrue(didThrowValidationError(new FileName("adad@adad"), "File name cannot contain a colon, forward slash, at sign, or open curly bracket."));
    Assert.assertTrue(didThrowValidationError(new FileName("adad{adad"), "File name cannot contain a colon, forward slash, at sign, or open curly bracket."));
    Assert.assertTrue(didThrowValidationError(new FileName("."), "File name cannot start with period"));
    Assert.assertTrue(didThrowValidationError(new FileName(".adadd"), "File name cannot start with period"));
    Assert.assertFalse(didThrowValidationError(new FileName("ad.add"), "File name cannot start with period"));
  }

  private boolean didThrowValidationError(Object o, String expectedMessage) {
    try {
      new InputValidation().validate(o);
    } catch (ClientErrorException e) {
      return (e.getMessage().contains(expectedMessage));
    }

    return false;
  }
}

