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

import {
  getColumnByName,
  getMeasuresForColumnType,
  isMeasureValidForColumnType
} from './aggregateUtils';

describe('aggregateUtils', () => {
  describe('getColumnByName', () => {
    it('should return column with name=columnName', () => {
      const allColumns = Immutable.fromJS([{name: 'foo'}]);
      expect(
        getColumnByName(allColumns, 'foo')
      ).to.equal(allColumns.get(0));
    });
  });

  describe('getMeasuresForColumnType', () => {
    it('should return all measures for INTEGER', () => {
      expect(getMeasuresForColumnType('INTEGER')).to.have.length(11);
    });

    it('should return only a few measures for TEXT', () => {
      expect(getMeasuresForColumnType('TEXT')).to.have.length(5);
    });
  });

  describe('isMeasureValidForColumnType', () => {
    it('should return true if measure has no columnTypes', () => {
      expect(isMeasureValidForColumnType('Count', 'INTEGER')).to.eql(true);
    });

    it('should return true if measure\'s columnTypes that includes given type', () => {
      expect(isMeasureValidForColumnType('Sum', 'INTEGER')).to.eql(true);
    });

    it('should return false if measure\'s columnTypes does not include given type', () => {
      expect(isMeasureValidForColumnType('Sum', 'TEXT')).to.eql(false);
    });
  });

});


