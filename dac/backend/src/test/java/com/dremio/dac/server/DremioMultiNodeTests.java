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
package com.dremio.dac.server;

/**
 * This is a JUnit 4 test category marking tests whose behavior relies on the "dremio_multinode"
 * system property. Note that when applied as "@Category(DremioMultiNodeTests.class)" to a class, it
 * will mark all sub-classes as well.
 *
 * <p>It is the equivalent of the JUnit 5 tag "dremio-multi-node-tests".
 */
public interface DremioMultiNodeTests {}
