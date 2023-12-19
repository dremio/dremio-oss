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
package com.dremio.exec.store.dfs;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class TestErrorInfo {

  @Test
  public void testGetJsonAndDeserializeInfo() {
    // Create an instance of TestErrorInfo
    TestInfo testErrorInfo = new TestInfo(404, "Not Found");

    // Serialize the TestErrorInfo to JSON using ErrorInfo.Util.getJson
    String json = ErrorInfo.Util.getJson(testErrorInfo);

    // Deserialize the JSON back to an ErrorInfo object using ErrorInfo.Util.getInfo
    TestInfo deserializedErrorInfo = ErrorInfo.Util.getInfo(json, TestInfo.class);

    // Assert that the deserialized ErrorInfo object is equal to the original TestErrorInfo
    assertThat(testErrorInfo.getErrorCode()).isEqualTo(deserializedErrorInfo.getErrorCode());
    assertThat(testErrorInfo.getErrorMessage()).isEqualTo(deserializedErrorInfo.getErrorMessage());
  }

  // Example implementation of ErrorInfo for testing purposes
  private static class TestInfo implements ErrorInfo {
    private int errorCode;
    private String errorMessage;

    public TestInfo() {
    }

    public TestInfo(int errorCode, String errorMessage) {
      this.errorCode = errorCode;
      this.errorMessage = errorMessage;
    }

    public int getErrorCode() {
      return errorCode;
    }

    public String getErrorMessage() {
      return errorMessage;
    }
  }

}
