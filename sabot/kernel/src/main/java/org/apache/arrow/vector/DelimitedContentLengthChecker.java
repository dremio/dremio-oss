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

package org.apache.arrow.vector;

import com.google.common.annotations.VisibleForTesting;

public class DelimitedContentLengthChecker implements ContentLengthChecker {
  private final int delimiterLength;
  private final int maxContentLength;
  private boolean allowOneOverflow;
  private final boolean actualAllowOneOverflow;

  private boolean isEmpty = true;
  private int totalContentLength = 0;

  /**
   * This calculator accommodates the space taken by the delimiter in its content length calculations
   */
  DelimitedContentLengthChecker(final int delimiterLength, final int maxContentLength, boolean allowOneOverflow) {
    this.delimiterLength = delimiterLength;
    this.maxContentLength = maxContentLength;
    this.allowOneOverflow = allowOneOverflow;
    this.actualAllowOneOverflow = allowOneOverflow;
  }

  @Override
  public boolean hasSpaceFor(final int contentLength) {
    final boolean includeDelimiterLengthOnAdd = !isEmpty;
    final int addedLengthToCheck = includeDelimiterLengthOnAdd ? (contentLength + delimiterLength) : contentLength;

    if (totalContentLength + addedLengthToCheck <= maxContentLength) {
      return true;
    } else if (allowOneOverflow) {
      allowOneOverflow = false;
      return true;
    }
    return false;
  }

  @Override
  @VisibleForTesting
  public void addToTotalLength(final int contentLength) {
    final boolean includeDelimiterLengthOnAdd = !isEmpty;

    if (includeDelimiterLengthOnAdd) {
      totalContentLength += delimiterLength;
    }

    totalContentLength += contentLength;

    isEmpty = false;
  }

  @Override
  public void reset() {
    totalContentLength = 0;
    isEmpty = true;
    allowOneOverflow = actualAllowOneOverflow;
  }
}
