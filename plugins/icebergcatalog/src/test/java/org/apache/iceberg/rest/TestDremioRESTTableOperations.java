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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.exec.store.iceberg.DremioFileIO;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestDremioRESTTableOperations {

  @Mock private DremioFileIO mockFileIO;

  @Mock private RESTTableOperations mockRestTableOperations;

  private DremioRESTTableOperations dremioRESTTableOperations;

  @Before
  public void setup() {
    dremioRESTTableOperations = new DremioRESTTableOperations(mockFileIO, mockRestTableOperations);
  }

  @After
  public void teardown() {
    dremioRESTTableOperations = null;
  }

  @Test
  public void testCurrentDelegates() {
    when(mockRestTableOperations.current()).thenReturn(null);
    dremioRESTTableOperations.current();
    verify(mockRestTableOperations, times(1)).current();
  }

  @Test
  public void testRefreshDelegates() {
    when(mockRestTableOperations.refresh()).thenReturn(null);
    dremioRESTTableOperations.refresh();
    verify(mockRestTableOperations, times(1)).refresh();
  }

  @Test
  public void testMetadataFileLocationDelegates() {
    when(mockRestTableOperations.metadataFileLocation(anyString())).thenReturn(null);
    dremioRESTTableOperations.metadataFileLocation(anyString());
    verify(mockRestTableOperations, times(1)).metadataFileLocation(anyString());
  }

  @Test
  public void testLocationProviderDelegates() {
    when(mockRestTableOperations.locationProvider()).thenReturn(null);
    dremioRESTTableOperations.locationProvider();
    verify(mockRestTableOperations, times(1)).locationProvider();
  }

  @Test
  public void testReturnedIO() {
    assertEquals(mockFileIO, dremioRESTTableOperations.io());
  }

  /** Unsupported methods */
  @Test
  public void testCommit() {
    assertThatThrownBy(() -> dremioRESTTableOperations.commit(null, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
