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
package com.dremio.services.nessie.proxy;

import static com.dremio.services.nessie.proxy.MockClientProducer.clientMock;
import static io.restassured.RestAssured.given;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.IcebergTable;

@NessieApiVersions(versions = NessieApiVersion.V2)
public class TestProxyToJavaClient {

  @RegisterExtension
  private static final NessieProxyJaxRsExtension proxy =
      new NessieProxyJaxRsExtension(MockClientProducer.class);

  private static final Branch BRANCH = Branch.of("main", "1122334455667788");

  private NessieApiV2 api;

  @BeforeEach
  void setupRestUri(@NessieClientUri URI uri) {
    RestAssured.baseURI = uri.toString();
    RestAssured.port = uri.getPort();
  }

  protected static RequestSpecification rest() {
    return given().when().baseUri(RestAssured.baseURI).basePath("").contentType(ContentType.JSON);
  }

  @BeforeEach
  void initApi(NessieClientFactory clientFactory) {
    this.api =
        (NessieApiV2)
            clientFactory.make((builder, apiVersion) -> builder.withApiCompatibilityCheck(false));
  }

  @Test
  void testForWriteMultiple() throws NessieNotFoundException {
    NessieApiV2 clientMock = clientMock();
    GetContentBuilder request = Mockito.mock(GetContentBuilder.class, RETURNS_SELF);
    when(clientMock.getContent()).thenReturn(request);
    when(request.getWithResponse()).thenReturn(GetMultipleContentsResponse.of(List.of(), BRANCH));

    api.getContent().refName("main").key(ContentKey.of("test")).forWrite(true).get();
    verify(request, times(1)).forWrite(eq(true));

    api.getContent().refName("main").key(ContentKey.of("test")).forWrite(false).get();
    verify(request, times(1)).forWrite(eq(false));
  }

  @Test
  void testForWriteSeveral() throws NessieNotFoundException {
    NessieApiV2 clientMock = clientMock();
    GetContentBuilder request = Mockito.mock(GetContentBuilder.class, RETURNS_SELF);
    when(clientMock.getContent()).thenReturn(request);
    when(request.getWithResponse()).thenReturn(GetMultipleContentsResponse.of(List.of(), BRANCH));

    rest().get("trees/main/contents?key=k1&key=k2&for-write=true").then().statusCode(200);
    verify(request, times(1)).forWrite(eq(true));

    rest().get("trees/main/contents?key=k1&key=k2").then().statusCode(200);
    verify(request, times(1)).forWrite(eq(false));
  }

  @Test
  void testForWriteSingle() throws NessieNotFoundException {
    NessieApiV2 clientMock = clientMock();
    GetContentBuilder request = Mockito.mock(GetContentBuilder.class, RETURNS_SELF);
    when(clientMock.getContent()).thenReturn(request);
    when(request.getSingle(any()))
        .thenReturn(ContentResponse.of(IcebergTable.of("loc", 1, 2, 3, 4), BRANCH));

    rest().get("trees/main/contents/testKey?for-write=true").then().statusCode(200);
    verify(request, times(1)).forWrite(eq(true));

    rest().get("trees/main/contents/testKey").then().statusCode(200);
    verify(request, times(1)).forWrite(eq(false));
  }
}
