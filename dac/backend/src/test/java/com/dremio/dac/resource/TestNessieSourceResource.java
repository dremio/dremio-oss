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
package com.dremio.dac.resource;

import static com.dremio.exec.ExecConstants.NESSIE_SOURCE_API;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.client.api.NessieApiV2;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.service.errors.NessieSourceNotValidException;
import com.dremio.dac.service.errors.SourceNotFoundException;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.plugins.s3.store.S3StoragePlugin;
import com.dremio.services.nessie.proxy.ProxyV2TreeResource;

@ExtendWith(MockitoExtension.class)
public class TestNessieSourceResource {

  @Mock
  private CatalogService catalogService;

  @InjectMocks
  private NessieSourceResource nessieSourceResource;

  @Mock
  private DataplanePlugin dataplanePlugin;

  @Mock
  private NessieApiV2 nessieApiV2;

  @Mock
  private S3StoragePlugin s3StoragePlugin;

  @Mock
  private UserException userException;

  @Mock
  private OptionManager optionManager;

  private final String sourceName = "MY_SOURCE";

  @Test
  public void testNessieSourceNotFound() {
    when(catalogService.getSource(sourceName)).thenThrow(userException);
    when(optionManager.getOption(NESSIE_SOURCE_API)).thenReturn(true);
    assertThatThrownBy(()->nessieSourceResource.handle(sourceName))
      .isInstanceOf(SourceNotFoundException.class);
  }


  @Test
  public void testNessieSourceSuccess() {
    when(catalogService.getSource(sourceName)).thenReturn(dataplanePlugin);
    when(dataplanePlugin.getNessieApi()).thenReturn(nessieApiV2);
    when(optionManager.getOption(NESSIE_SOURCE_API)).thenReturn(true);
    ProxyV2TreeResource expected = nessieSourceResource.handle(sourceName);
    assertTrue(expected instanceof ProxyV2TreeResource);
  }

  @Test
  public void testNessieSourceNotAcceptable() {
    when(catalogService.getSource(sourceName)).thenReturn(s3StoragePlugin);
    when(optionManager.getOption(NESSIE_SOURCE_API)).thenReturn(true);
    assertThatThrownBy(()->nessieSourceResource.handle(sourceName))
      .isInstanceOf(NessieSourceNotValidException.class);
  }
}
