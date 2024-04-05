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
package com.dremio.service.namespace;

import com.dremio.service.namespace.dataset.DatasetNamespaceService;
import com.dremio.service.namespace.folder.FolderNamespaceService;
import com.dremio.service.namespace.function.FunctionNamespaceService;
import com.dremio.service.namespace.home.HomeNamespaceService;
import com.dremio.service.namespace.source.SourceNamespaceService;
import com.dremio.service.namespace.space.SpaceNamespaceService;
import com.dremio.service.namespace.split.SplitNamespaceService;

/**
 * Namespace operations from DAC. For generic entity operations, use EntityNamespaceService. For
 * specific entity operations, use one of the specific entity interfaces like
 * SourceNamespaceService.
 */
public interface NamespaceService
    extends EntityNamespaceService,
        SourceNamespaceService,
        SpaceNamespaceService,
        FunctionNamespaceService,
        HomeNamespaceService,
        FolderNamespaceService,
        DatasetNamespaceService,
        SplitNamespaceService {
  interface Factory {
    /**
     * Return a namespace service for a given user. Note that this is for usernames and users only,
     * if roles are to be supported, use #get(NamespaceIdentity) instead.
     *
     * @param userName a valid user name
     * @return a namespace service instance
     * @throws NullPointerException if {@code userName} is null
     * @throws IllegalArgumentException if {@code userName} is invalid
     */
    NamespaceService get(String userName);

    NamespaceService get(NamespaceIdentity identity);
  }
}
