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

import static com.dremio.plugins.azure.ChecksumVerifyingCompletionHandler.CHECKSUM_RESPONSE_HEADER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;

import org.apache.commons.codec.digest.DigestUtils;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.Response;
import org.junit.jupiter.api.Test;

import io.netty.buffer.Unpooled;

class TestChecksumVerifyingCompletionHandler {

  @Test
  void failedRequestIgnoresMD5() throws Exception {
    ChecksumVerifyingCompletionHandler handler =
      new ChecksumVerifyingCompletionHandler(Unpooled.buffer(32), 0);
    HttpResponseStatus status = mock(HttpResponseStatus.class);
    String responseBody = "some error message";
    when(status.getStatusCode()).thenReturn(500);
    handler.onStatusReceived(status);
    Response response = mock(Response.class);
    when(response.getResponseBody()).thenReturn(responseBody);
    when(response.getHeader(CHECKSUM_RESPONSE_HEADER)).thenReturn("0");
    assertThatThrownBy(() -> handler.onCompleted(response))
      .isInstanceOf(RuntimeException.class)
      .hasMessage(responseBody);
  }

  @Test
  void mismatchedMD5HeaderFails() throws Exception {
    HttpResponseStatus status = mock(HttpResponseStatus.class);
    when(status.getStatusCode()).thenReturn(206);

    String responseBody = "totally valid parquet";
    HttpResponseBodyPart responsePart = mock(HttpResponseBodyPart.class);
    when(responsePart.getBodyByteBuffer()).thenReturn(ByteBuffer.wrap(responseBody.getBytes()));

    Response response = mock(Response.class);
    when(response.getResponseBody()).thenReturn(responseBody);
    when(response.getHeader(CHECKSUM_RESPONSE_HEADER)).thenReturn("invalid");

    ChecksumVerifyingCompletionHandler handler =
      new ChecksumVerifyingCompletionHandler(Unpooled.buffer(32), 0);
    handler.onStatusReceived(status);

    assertThatThrownBy(() -> handler.onCompleted(response))
      .isInstanceOf(IOException.class)
      .hasMessage("mismatched MD5 checksum: got 1B2M2Y8AsgTpgAmY7PhCfg==, expected invalid");
  }

  @Test
  void missingMD5HeaderFails() throws Exception {
    HttpResponseStatus status = mock(HttpResponseStatus.class);
    when(status.getStatusCode()).thenReturn(206);

    String responseBody = "totally valid parquet";
    HttpResponseBodyPart responsePart = mock(HttpResponseBodyPart.class);
    when(responsePart.getBodyByteBuffer()).thenReturn(ByteBuffer.wrap(responseBody.getBytes()));

    Response response = mock(Response.class);
    when(response.getResponseBody()).thenReturn(responseBody);

    ChecksumVerifyingCompletionHandler handler =
      new ChecksumVerifyingCompletionHandler(Unpooled.buffer(32), 0);
    handler.onStatusReceived(status);

    assertThatThrownBy(() -> handler.onCompleted(response))
      .isInstanceOf(IOException.class)
      .hasMessage("MD5 checksum requested, but response header missing");
  }

  @Test
  void ignorePreviouslyReceivedBodyPartsOnReset() throws Exception {
    HttpResponseStatus initialStatus = mock(HttpResponseStatus.class);
    when(initialStatus.getStatusCode()).thenReturn(500);

    HttpResponseStatus secondTryStatus = mock(HttpResponseStatus.class);
    when(secondTryStatus.getStatusCode()).thenReturn(206);

    String initialBodyBytes = "some bytes we don't want to see ";
    String secondTryBodyBytes = "some bytes we do want to see    ";

    HttpResponseBodyPart initialResponsePart = mock(HttpResponseBodyPart.class);
    when(initialResponsePart.getBodyByteBuffer()).thenReturn(
      ByteBuffer.wrap(initialBodyBytes.getBytes()));

    HttpResponseBodyPart secondTryResponsePart = mock(HttpResponseBodyPart.class);
    when(secondTryResponsePart.getBodyByteBuffer()).thenReturn(
      ByteBuffer.wrap(secondTryBodyBytes.getBytes()));

    Response response = mock(Response.class);
    when(response.getHeader(CHECKSUM_RESPONSE_HEADER)).thenReturn(
      Base64.getEncoder().encodeToString(DigestUtils.md5(secondTryBodyBytes.getBytes())));

    ChecksumVerifyingCompletionHandler handler =
      new ChecksumVerifyingCompletionHandler(Unpooled.buffer(32), 0);
    handler.onBodyPartReceived(initialResponsePart);
    handler.onStatusReceived(initialStatus);
    handler.reset();
    handler.onStatusReceived(secondTryStatus);
    handler.onBodyPartReceived(secondTryResponsePart);
    handler.onCompleted(response);
  }

}
