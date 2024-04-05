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
package com.dremio.common.exceptions;

import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.junit.Assert;
import org.junit.Test;

/** Test JsonAdditionalExceptionContext serialize/deserialize functionality */
public class TestJsonAdditionalExceptionContext {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger("--ignore.as.this.is.for.testing.exceptions--");

  private static final String TEST_DATA = "test data";

  @JsonTypeName("test-context")
  private static class TestContext extends JsonAdditionalExceptionContext {
    private String data;

    public TestContext(@JsonProperty("data") String data) {
      this.data = data;
    }

    public String getData() {
      return data;
    }

    @Override
    public ErrorType getErrorType() {
      return ErrorType.FUNCTION;
    }

    public static TestContext fromUserException(UserException ex) {
      Preconditions.checkState(ex.getErrorType() == ErrorType.FUNCTION, "exception type mismatch");
      return JsonAdditionalExceptionContext.fromUserException(TestContext.class, ex);
    }
  }

  @Test
  public void testSerializeDeserialize() throws Exception {
    UserException ex =
        UserException.functionError()
            .message("test")
            .setAdditionalExceptionContext(new TestContext(TEST_DATA))
            .build(logger);
    Assert.assertTrue(ex.getRawAdditionalExceptionContext() != null);
    Assert.assertTrue(TEST_DATA.equals(TestContext.fromUserException(ex).getData()));

    // test leaving AdditionalExceptionContext as null
    ex = UserException.functionError().message("test").build(logger);
    Assert.assertTrue(ex.getRawAdditionalExceptionContext() == null);
    Assert.assertTrue(TestContext.fromUserException(ex) == null);
  }
}
