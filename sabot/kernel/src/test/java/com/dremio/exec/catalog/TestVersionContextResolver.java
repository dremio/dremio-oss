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
package com.dremio.exec.catalog;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.store.ConnectionRefusedException;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.UnAuthenticatedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.NessieForbiddenException;

@ExtendWith(MockitoExtension.class)
public class TestVersionContextResolver {

  @Mock private PluginRetriever pluginRetriever;

  @Test
  public void testNessieForbiddenException() {
    doThrow(new NessieForbiddenException(mock(NessieError.class)))
        .when(pluginRetriever)
        .getPlugin(anyString(), anyBoolean());
    assertThrows(
        NessieForbiddenException.class,
        () ->
            new VersionContextResolverImpl(pluginRetriever)
                .resolveVersionContext("mysource", mock(VersionContext.class)));
  }

  @Test
  public void testNessieReferenceNotFoundException() {
    String sourceName = "mySource";
    final FakeVersionedPlugin storagePlugin = mock(FakeVersionedPlugin.class);
    final VersionContext versionContext = VersionContext.ofBranch("testBranch");
    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(pluginRetriever.getPlugin(sourceName, false)).thenReturn(managedStoragePlugin);
    when(managedStoragePlugin.getPlugin()).thenReturn(storagePlugin);
    when(storagePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(storagePlugin.unwrap(VersionedPlugin.class)).thenReturn(storagePlugin);
    doThrow(new ReferenceNotFoundException())
        .when(storagePlugin)
        .resolveVersionContext(versionContext);
    assertThrows(
        VersionNotFoundInNessieException.class,
        () ->
            new VersionContextResolverImpl(pluginRetriever)
                .resolveVersionContext(sourceName, versionContext));
  }

  @Test
  public void testNessieReferenceConflictException() {
    String sourceName = "mySource";
    final FakeVersionedPlugin storagePlugin = mock(FakeVersionedPlugin.class);
    final VersionContext versionContext = VersionContext.ofBranch("testBranch");
    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(pluginRetriever.getPlugin(sourceName, false)).thenReturn(managedStoragePlugin);
    when(managedStoragePlugin.getPlugin()).thenReturn(storagePlugin);
    when(storagePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(storagePlugin.unwrap(VersionedPlugin.class)).thenReturn(storagePlugin);
    doThrow(new ReferenceConflictException())
        .when(storagePlugin)
        .resolveVersionContext(versionContext);
    assertThrows(
        RuntimeException.class,
        () ->
            new VersionContextResolverImpl(pluginRetriever)
                .resolveVersionContext(sourceName, versionContext));
  }

  @Test
  public void testNessieNoDefaultBranchException() {
    String sourceName = "mySource";
    final FakeVersionedPlugin storagePlugin = mock(FakeVersionedPlugin.class);
    final VersionContext versionContext = VersionContext.ofBranch("testBranch");
    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(pluginRetriever.getPlugin(sourceName, false)).thenReturn(managedStoragePlugin);
    when(managedStoragePlugin.getPlugin()).thenReturn(storagePlugin);
    when(storagePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(storagePlugin.unwrap(VersionedPlugin.class)).thenReturn(storagePlugin);
    doThrow(new NoDefaultBranchException())
        .when(storagePlugin)
        .resolveVersionContext(versionContext);
    assertThrows(
        VersionNotFoundInNessieException.class,
        () ->
            new VersionContextResolverImpl(pluginRetriever)
                .resolveVersionContext(sourceName, versionContext));
  }

  @Test
  public void testNessieConnectionException() {
    String sourceName = "mySource";
    final FakeVersionedPlugin storagePlugin = mock(FakeVersionedPlugin.class);
    final VersionContext versionContext = VersionContext.ofBranch("testBranch");
    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(pluginRetriever.getPlugin(sourceName, false)).thenReturn(managedStoragePlugin);
    when(managedStoragePlugin.getPlugin()).thenReturn(storagePlugin);
    when(storagePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(storagePlugin.unwrap(VersionedPlugin.class)).thenReturn(storagePlugin);
    doThrow(new ConnectionRefusedException())
        .when(storagePlugin)
        .resolveVersionContext(versionContext);
    assertThrows(
        RuntimeException.class,
        () ->
            new VersionContextResolverImpl(pluginRetriever)
                .resolveVersionContext(sourceName, versionContext));
  }

  @Test
  public void testNessieUnauthenticatedException() {
    String sourceName = "mySource";
    final FakeVersionedPlugin storagePlugin = mock(FakeVersionedPlugin.class);
    final VersionContext versionContext = VersionContext.ofBranch("testBranch");
    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(pluginRetriever.getPlugin(sourceName, false)).thenReturn(managedStoragePlugin);
    when(managedStoragePlugin.getPlugin()).thenReturn(storagePlugin);
    when(storagePlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(storagePlugin.unwrap(VersionedPlugin.class)).thenReturn(storagePlugin);
    doThrow(new UnAuthenticatedException())
        .when(storagePlugin)
        .resolveVersionContext(versionContext);
    assertThrows(
        RuntimeException.class,
        () ->
            new VersionContextResolverImpl(pluginRetriever)
                .resolveVersionContext(sourceName, versionContext));
  }

  @Test
  public void testNessieReferenceException() {
    doThrow(NullPointerException.class).when(pluginRetriever).getPlugin(anyString(), anyBoolean());
    assertThrows(
        RuntimeException.class,
        () ->
            new VersionContextResolverImpl(pluginRetriever)
                .resolveVersionContext("mysource", mock(VersionContext.class)));
  }

  /** Fake Versioned Plugin interface for test */
  private interface FakeVersionedPlugin extends VersionedPlugin, StoragePlugin {}
}
