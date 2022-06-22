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
package com.dremio.plugins;

import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;

import com.dremio.context.RequestContext;
import com.dremio.exec.catalog.ResolvedVersionContext;

public class TestNessieClientImpl {

  private static final List<String> CATALOG_KEY = Arrays.asList("test1", "test2");
  private static final List<String> CATALOG_KEY_2 = Arrays.asList("test3", "test4");
  private static final ContentKey CONTENT_KEY = ContentKey.of(CATALOG_KEY);
  private static final Content CONTENT = IcebergTable.of("test", 0L, 0, 0,0);
  private static final ResolvedVersionContext VERSION = ResolvedVersionContext.ofBranch("main", "0123456789abcdeff");
  private static final ResolvedVersionContext VERSION_2 = ResolvedVersionContext.ofBranch("dev", "0123456789bbcdeff");
  private static final Map<ContentKey, Content> CONTENT_MAP = new HashMap<ContentKey, Content>() {{
    put(CONTENT_KEY, CONTENT);
  }};

  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  private NessieApiV1 nessieApiV1;

  @InjectMocks
  private NessieClientImpl nessieClient;

  private GetContentBuilder builder;

  @Before
  public void setUp() {
    builder = mock(GetContentBuilder.class, RETURNS_SELF);
    when(nessieApiV1.getContent()).thenReturn(builder);
    nessieClient.setRequestContext(RequestContext.current());
  }

  @Test
  public void testMetadataLocationCache() throws NessieNotFoundException {
    when(builder.get()).thenReturn(CONTENT_MAP);
    nessieClient.getMetadataLocation(CATALOG_KEY, VERSION);
    verify(builder, times(1)).get();
    nessieClient.getMetadataLocation(CATALOG_KEY, VERSION);
    verify(builder, times(1)).get();
  }

  @Test
  public void testMetadataLocationCacheWithDifferentVersion() throws NessieNotFoundException {
    when(builder.get()).thenReturn(generateRandomMap());

    nessieClient.getMetadataLocation(CATALOG_KEY, VERSION);
    verify(builder, times(1)).get();

    nessieClient.getMetadataLocation(CATALOG_KEY, VERSION_2);
    verify(builder, times(2)).get();
  }

  @Test
  public void testMetadataLocationCacheWithDifferentCatalogKey() throws NessieNotFoundException {
    when(builder.get()).thenReturn(CONTENT_MAP);

    nessieClient.getMetadataLocation(CATALOG_KEY, VERSION);
    verify(builder, times(1)).get();

    nessieClient.getMetadataLocation(CATALOG_KEY_2, VERSION);
    verify(builder, times(2)).get();
  }

  private Map<ContentKey, Content> generateRandomMap(){
    Map<ContentKey, Content> map = new HashMap<>();
    ContentKey key = ContentKey.of(Arrays.asList(generateRandomString(), generateRandomString()));
    Content content = IcebergTable.of(generateRandomString(), 0L, 0, 0,0);
    map.put(key, content);
    return map;
  }

  private String generateRandomString(){
    int leftLimit = 97; // letter 'a'
    int rightLimit = 122; // letter 'z'
    int targetStringLength = 5;
    Random random = new Random();
    StringBuilder buffer = new StringBuilder(targetStringLength);
    for (int i = 0; i < targetStringLength; i++) {
      int randomLimitedInt = leftLimit + (int)
        (random.nextFloat() * (rightLimit - leftLimit + 1));
      buffer.append((char) randomLimitedInt);
    }
    return buffer.toString();
  }
}
