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
package com.dremio.service;

import javax.inject.Provider;

public interface BindingCreator {

  <IFACE> IFACE bindSelf(IFACE impl);

  <IFACE> IFACE bind(Class<IFACE> iface, IFACE impl);

  <IFACE> boolean bindIfUnbound(Class<IFACE> iface, IFACE impl);

  <IFACE> void replace(Class<IFACE> iface, IFACE impl);

  <IFACE> void bindSelf(Class<IFACE> iface);

  <IFACE> void bind(Class<IFACE> iface, Class<? extends IFACE> impl);

  <IFACE> void replace(Class<IFACE> iface, Class<? extends IFACE> impl);

  <IFACE> void bindProvider(Class<IFACE> iface, Provider<? extends IFACE> provider);

  <IFACE> void replaceProvider(Class<IFACE> iface, Provider<? extends IFACE> provider);
}
