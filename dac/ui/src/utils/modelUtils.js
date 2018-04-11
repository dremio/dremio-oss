/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
class ModelUtils {

  isNewDataset(dataset, mode) {
    if (!dataset) {
      return false;
    }
    const tableId = dataset.getIn(['fullPath', -1]);
    if (!tableId) {
      return false;
    }
    return tableId === '"UNTITLED"' || mode !== 'edit';
  }

  /**
   * Returns true in case when dataset is created or saved from another dataset and not related to "New Query"
   *
   * @param {Immutable.Map} dataset
   * @returns {Boolean}
   */
  isNamedDataset(dataset) {
    return dataset &&
      !dataset.get('isNewQuery') &&
      dataset.get('displayFullPath') &&
      dataset.getIn(['displayFullPath', 0]) !== 'tmp';
  }

}

const modelUtils = new ModelUtils();

export default modelUtils;
