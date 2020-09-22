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
package com.dremio.exec.server.options;

import static com.dremio.exec.ExecConstants.ENABLE_VERBOSE_ERRORS;
import static com.dremio.exec.ExecConstants.ENABLE_VERBOSE_ERRORS_KEY;
import static com.dremio.exec.ExecConstants.SLICE_TARGET;
import static com.dremio.exec.ExecConstants.SLICE_TARGET_OPTION;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.options.OptionValidator;
import com.dremio.options.OptionValidatorListing;
import com.dremio.test.DremioTest;
import com.dremio.test.UserExceptionMatcher;

public class TestOptionValidatorListing extends DremioTest {
  private static OptionValidatorListing optionValidatorListing;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setupClass() throws Exception {
    optionValidatorListing = new OptionValidatorListingImpl(CLASSPATH_SCAN_RESULT);
  }

  @Test
  public void testGetValidator() throws Exception {
    OptionValidator validator = optionValidatorListing.getValidator(ENABLE_VERBOSE_ERRORS_KEY);
    assertSame(ENABLE_VERBOSE_ERRORS, validator);
  }

  @Test
  public void testGetValidatorInvalid() throws Exception {
    String invalid_name = "invalid_name";
    thrown.expect(new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.VALIDATION,
      String.format("The option '%s' does not exist.", invalid_name)));
    optionValidatorListing.getValidator(invalid_name);
  }

  @Test
  public void testIsValid() throws Exception {
    assertTrue(optionValidatorListing.isValid(SLICE_TARGET));
    assertFalse(optionValidatorListing.isValid("invalid_name"));
  }

  @Test
  public void testGetValidatorList() throws Exception {
    Collection<OptionValidator> validators = optionValidatorListing.getValidatorList();
    // Spot check some option validators
    assertTrue(validators.contains(ENABLE_VERBOSE_ERRORS));
    assertTrue(validators.contains(SLICE_TARGET_OPTION));
  }
}
