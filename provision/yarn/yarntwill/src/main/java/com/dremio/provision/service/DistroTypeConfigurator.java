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
package com.dremio.provision.service;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import com.dremio.provision.DistroType;
import com.dremio.provision.Property;

/**
 * IFace to unify all the DistroType based defaults
 */

public interface DistroTypeConfigurator {

    EnumSet<DistroType> getSupportedTypes();

    boolean isSecure();

    Map<String, String> getAllDefaults();

    Map<String, String> getAllToShowDefaults();

    void mergeProperties(List<Property> originalProperties);
}
