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
import * as saveActions from './save';

describe('Dataset save actions', () => {
  describe('#deleteOldDatasetVersions', () => {
    it('should return type=DELETE_OLD_DATASET_VERSIONS', () => {
      expect(
        saveActions.deleteOldDatasetVersions('123', Immutable.List()).type
      ).to.equal(
        saveActions.DELETE_OLD_DATASET_VERSIONS
      );
    });
    it('should include meta.entityRemovePaths for datasets and tableData in history except currentVersion', () => {
      const currentVersion = '123';
      const historyItems = Immutable.fromJS(
        [{datasetVersion: '123'}, {datasetVersion: '456'}, {datasetVersion: '789'}]
      );
      const result = saveActions.deleteOldDatasetVersions(currentVersion, historyItems);
      expect(result.meta.entityRemovePaths).to.eql([
        ['datasetUI', '456'], ['tableData', '456'], ['fullDataset', '456'], ['history', '456'],
        ['datasetUI', '789'], ['tableData', '789'], ['fullDataset', '789'], ['history', '789']
      ]);
    });
  });
});
