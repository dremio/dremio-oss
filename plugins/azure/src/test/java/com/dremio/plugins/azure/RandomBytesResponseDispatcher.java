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

package com.dremio.plugins.azure;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Random;

import org.asynchttpclient.HttpResponseBodyPart;

/**
 * This class breaks the response in multiple parts, and dispatches one by one
 */
public class RandomBytesResponseDispatcher {
  private byte[] allBytes;
  private Random random = new Random();
  private int nextStartIndex = 0;

  public RandomBytesResponseDispatcher(byte[] allBytes) {
    this.allBytes = allBytes;
  }

  public HttpResponseBodyPart getNextBodyPart() {
    HttpResponseBodyPart responseBodyPart = mock(HttpResponseBodyPart.class);
    when(responseBodyPart.getBodyPartBytes()).thenReturn(getNextChunk());
    return responseBodyPart;
  }

  public byte[] getNextChunk() {
    int batchSize = random.nextInt(5) * 128;
    int endIndex = Math.min(nextStartIndex + batchSize, allBytes.length - 1);
    byte[] chunk = Arrays.copyOfRange(allBytes, nextStartIndex, endIndex);
    nextStartIndex = endIndex;
    return chunk;
  }

  public boolean isNotFinished() {
    return nextStartIndex < allBytes.length - 1;
  }
}
