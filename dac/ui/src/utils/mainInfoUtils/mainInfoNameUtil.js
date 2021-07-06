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

export function checkIfUserShouldGetDeadLink() {
  return false;
}

export function getHref(entity, context) {
  const fileType = entity.get('fileType');
  if (entity.get('fileType') === 'file') {
    if (entity.get('queryable')) {
      return entity.getIn(['links', 'query']);
    }
    return {
      ...context.location,
      state: {
        modal: 'DatasetSettingsModal',
        tab: 'format',
        entityType: entity.get('entityType'),
        entityId: entity.get('id'),
        fullPath: entity.get('filePath'),
        query: {then: 'query'},
        isHomePage: true
      }
    };
  }
  if (fileType === 'folder') {
    if (entity.get('queryable')) {
      return entity.getIn(['links', 'query']);
    }
    return entity.getIn(['links', 'self']);
  }
  return {
    ...context.location,
    state: {
      ...context.location.state,
      originalDatasetVersion: entity.get('datasetConfig') && entity.getIn(['datasetConfig', 'version'])
    },
    pathname: entity.getIn(['links', 'query'])
  };
}
