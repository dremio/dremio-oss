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

import com.dremio.http.BufferBasedCompletionHandler;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Base64;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** verifies content MD5 checksum if provided in the http-header <code>Content-MD5</code>. */
public class ChecksumVerifyingCompletionHandler extends BufferBasedCompletionHandler {

  private static final Logger logger =
      LoggerFactory.getLogger(ChecksumVerifyingCompletionHandler.class);
  public static final String CHECKSUM_RESPONSE_HEADER = "Content-MD5";
  public static final String HASH_ALGORITHM = "MD5";

  public ChecksumVerifyingCompletionHandler(ByteBuf outputBuffer, int dstOffset) {
    super(outputBuffer, dstOffset);
  }

  @Override
  public Response onCompleted(Response response) throws Exception {
    if (isRequestFailed()) {
      logger.error(
          "Error response received {} {}", response.getStatusCode(), response.getResponseBody());
      throw new RuntimeException(response.getResponseBody());
    }
    String expectedMD5 = response.getHeader(CHECKSUM_RESPONSE_HEADER);
    if (expectedMD5 != null) {
      MessageDigest digest = MessageDigest.getInstance(HASH_ALGORITHM);
      digest.update(getBodyBytes());
      String checksum = Base64.getEncoder().encodeToString(digest.digest());
      if (!checksum.equals(expectedMD5)) {
        throw new IOException(
            String.format("mismatched MD5 checksum: got %s, expected %s", checksum, expectedMD5));
      }
    } else {
      throw new IOException("MD5 checksum requested, but response header missing");
    }
    return response;
  }
}
