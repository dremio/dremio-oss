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

import java.util.List;

import com.dremio.service.Service;
import com.dremio.service.namespace.NamespaceAttribute;

/**
 * Factory for creating View Creators
 */
public interface ViewCreatorFactory extends Service {
  /**
   * Used to create Views in a space
   */
  interface ViewCreator {
    /**
     * Creates a view
     * @param path
     * @param sql
     * @param sqlContext
     * @param isVersionedSource
     * @param attributes
     */
    void createView(List<String> path, String sql, List<String> sqlContext, boolean isVersionedSource, NamespaceAttribute... attributes);

    /**
     * Updates a view
     * @param path
     * @param sql
     * @param sqlContext
     * @param attributes
     */
    void updateView(List<String> path, String sql, List<String> sqlContext, NamespaceAttribute... attributes);

    /**
     * Drops a view
     * @param path
     */
    void dropView(List<String> path);
  }

  /**
   * get a ViewCreator for a particular user
   * @param userName
   * @return the ViewCreator
   */
  ViewCreator get(String userName);
}
