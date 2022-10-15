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
package com.dremio.common.expression;

import com.dremio.common.types.TypeProtos.MajorType;
import com.google.common.collect.Range;
import com.google.errorprone.annotations.FormatMethod;

public interface ErrorCollector extends AutoCloseable {

    public void addGeneralError(String error);

    @FormatMethod
    public void addGeneralError(String format, Object... args);

    public void addUnexpectedArgumentType(String name, MajorType actual, MajorType[] expected, int argumentIndex);

    public void addUnexpectedArgumentCount(int actual, Range<Integer> expected);

    public void addUnexpectedArgumentCount(int actual, int expected);

    public void addNonNumericType(MajorType actual);

    public void addUnexpectedType(int index, MajorType actual);

    public void addExpectedConstantValue(int actual, String s);

    boolean hasErrors();

    public int getErrorCount();

    public void close();
    String toErrorString();
}
