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
import Immutable from 'immutable';

import modelUtils from './modelUtils';

describe('exploreUtils', () => {
  describe('check isNewDataset', () => {
    it('should return false when no tableName', () => {
      expect(modelUtils.isNewDataset(null, 'new')).to.be.false;
    });

    it('should return true', () => {
      const dataset = Immutable.fromJS({
        fullPath: ['foo', 'bar']
      });
      expect(modelUtils.isNewDataset(dataset, 'new')).to.be.true;
    });

    it('should return true for untitled', () => {
      const dataset = Immutable.fromJS({
        fullPath: ['tmp', 'UNTITLED']
      });
      expect(modelUtils.isNewDataset(dataset)).to.be.true;
    });

    it('should return false', () => {
      const dataset = Immutable.fromJS({
        fullPath: ['foo', 'name']
      });
      expect(modelUtils.isNewDataset(dataset)).to.be.fase;
    });
  });

  describe('#isNamedDataset', () => {
    it('should return true when displayFullPath of dataset is not the same as temporary', () => {
      const dataset = Immutable.fromJS({
        displayFullPath: ['ds']
      });
      expect(modelUtils.isNamedDataset(dataset)).to.be.true;
    });

    it('should return false when displayFullPath of dataset is related to tmp', () => {
      const dataset = Immutable.fromJS({
        displayFullPath: ['tmp']
      });
      expect(modelUtils.isNamedDataset(dataset)).to.be.false;
    });
  });
});
