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
package com.dremio.exec.store.hive.exec;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import com.dremio.io.FSInputStream;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TestFSInputStreamWrapper {

  @Mock
  private FSInputStream fsInputStream;

  private FSInputStreamWrapper fsInputStreamWrapper;

  @Before
  public void setUp() throws Exception {
    this.fsInputStreamWrapper = new FSInputStreamWrapper(fsInputStream);
  }

  @Test
  public void testCloseDelegation() throws IOException {
    fsInputStreamWrapper.close();
    verify(fsInputStream).close();
  }

  @Test
  public void testCloseThrowsIOException() throws IOException {
    doThrow(IOException.class).when(fsInputStream).close();
    assertThatThrownBy(() -> fsInputStreamWrapper.close()).isInstanceOf(IOException.class);
  }
}
