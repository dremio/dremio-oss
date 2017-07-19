/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.dfs;

import static com.dremio.common.TestProfileHelper.assumeNonMaprProfile;
import static com.dremio.exec.store.dfs.FSErrorTestUtils.newFSError;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;

import javassist.Modifier;

/**
 * Test to verify how FileSystemWrapper handle {@code FSError}
 */
@RunWith(Parameterized.class)
public class TestFileSystemWrapperFSError {
  @Parameters(name = "method: {0}")
  public static Object[] methodsToTest() {
    List<Method> methods = FluentIterable
        .of(FileSystem.class.getDeclaredMethods())
        .filter(new Predicate<Method>() {
          @Override
          public boolean apply(Method input) {
            // TODO(DX-7629): Remove this switch-case
            switch (input.getName()) {
              case "setStoragePolicy":
              case "unsetStoragePolicy":
              case "getStoragePolicy":
              case "getAllStoragePolicies":
                return false;
            }
            if (Modifier.isStatic(input.getModifiers())) {
              return false;
            }
            if (!Modifier.isPublic(input.getModifiers())) {
              return false;
            }
            return Arrays.asList(input.getExceptionTypes()).contains(IOException.class);
          }
        }).toList();

    return methods.toArray();
  }

  private final Method method;

  public TestFileSystemWrapperFSError(Method method) {
    this.method = method;
  }

  @Test
  public void test() throws Exception {
    assumeNonMaprProfile();
    final IOException ioException = new IOException("test io exception");
    final FSError fsError = newFSError(ioException);
    FileSystem underlyingFS = mock(FileSystem.class, new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (!invocation.getMethod().getName().equals("getScheme")) {
          throw fsError;
        }
        return "mockfs";
      }
    });
    Configuration conf = new Configuration(false);
    FileSystemWrapper fsw = new FileSystemWrapper(conf, underlyingFS, null);
    Object[] params = FSErrorTestUtils.getDummyArguments(method);
    try {
      method.invoke(fsw, params);
    } catch(InvocationTargetException e) {
      assertThat(e.getTargetException(), is(instanceOf(IOException.class)));
      assertThat((IOException) e.getTargetException(), is(sameInstance(ioException)));
    }
  }
}
