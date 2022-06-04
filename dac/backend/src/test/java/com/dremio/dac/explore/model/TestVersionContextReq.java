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

import static com.dremio.dac.explore.model.VersionContextReq.VersionContextType.BRANCH;
import static com.dremio.dac.explore.model.VersionContextReq.VersionContextType.COMMIT;
import static com.dremio.dac.explore.model.VersionContextReq.VersionContextType.TAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class TestVersionContextReq {

  private static final String BRANCH_TYPE = "BRANCH";
  private static final String TAG_TYPE = "TAG";
  private static final String COMMIT_TYPE = "COMMIT";
  private static final String BRANCH_VALUE = "branchName";
  private static final String TAG_VALUE = "tagName";
  private static final String COMMIT_VALUE = "01234567890abcdef";

  @Test
  public void tryParseBranch() {
    VersionContextReq actual = VersionContextReq.tryParse(BRANCH_TYPE, BRANCH_VALUE);
    VersionContextReq expected = new VersionContextReq(BRANCH, BRANCH_VALUE);
    assertThat(expected)
      .usingRecursiveComparison()
      .isEqualTo(actual);
  }

  @Test
  public void tryParseTag() {
    VersionContextReq actual = VersionContextReq.tryParse(TAG_TYPE, TAG_VALUE);
    VersionContextReq expected = new VersionContextReq(TAG, TAG_VALUE);
    assertThat(expected)
      .usingRecursiveComparison()
      .isEqualTo(actual);
  }

  @Test
  public void tryParseCommit() {
    VersionContextReq actual = VersionContextReq.tryParse(COMMIT_TYPE, COMMIT_VALUE);
    VersionContextReq expected = new VersionContextReq(COMMIT, COMMIT_VALUE);
    assertThat(expected)
      .usingRecursiveComparison()
      .isEqualTo(actual);
  }

  @Test
  public void tryParseLowercase() {
    VersionContextReq actual = VersionContextReq.tryParse("branch", BRANCH_VALUE);
    VersionContextReq expected = new VersionContextReq(BRANCH, BRANCH_VALUE);
    assertThat(expected)
      .usingRecursiveComparison()
      .isEqualTo(actual);
  }

  @Test
  public void tryParseBadType(){
    assertThatThrownBy(() -> VersionContextReq.tryParse("BAD_TYPE", BRANCH_VALUE))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void tryParseNullType() {
    VersionContextReq actual = VersionContextReq.tryParse(null, BRANCH_VALUE);
    assertThat(actual).isNull();
  }

  @Test
  public void tryParseEmptyType() {
    VersionContextReq actual = VersionContextReq.tryParse("", BRANCH_VALUE);
    assertThat(actual).isNull();
  }

  @Test
  public void tryParseNullValue() {
    VersionContextReq actual = VersionContextReq.tryParse(BRANCH_TYPE, null);
    assertThat(actual).isNull();
  }

  @Test
  public void tryParseEmptyValue() {
    VersionContextReq actual = VersionContextReq.tryParse(BRANCH_TYPE, "");
    assertThat(actual).isNull();
  }

}
