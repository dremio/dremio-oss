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
import filterMappers from './filterMappers.js';

describe('filterMappers', () => {

  describe('getCommonValues', () => {
    it('should return commonValues', () => {
      const result = filterMappers.getCommonFilterValues({
        keepNull: false
      }, Immutable.Map({columnName: 'age', transformType: 'keeponly'}));
      expect(result).to.eql({
        type: 'filter',
        sourceColumnName: 'age',
        keepNull: false,
        exclude: false
      });
    });
  });

  describe('mapFilterExcludeRange', () => {
    it('should return correct values', () => {
      const values = {
        lowerBound: 2,
        upperBound: 4,
        keepNull:false,
        lowerBoundInclusive: true,
        upperBoundInclusive: false,
        replacementType: 'INTEGER',
        replacementValue: '1'
      };
      expect(filterMappers.mapFilterExcludeRange(values, 'INTEGER')).to.eql({
        type: 'Range',
        lowerBound: 2,
        upperBound: 4,
        lowerBoundInclusive: true,
        upperBoundInclusive: false,
        dataType: 'INTEGER'
      });
    });
  });

  describe('mapFilterExcludeValues', () => {
    it('should return correct values', () => {
      const values = {
        replaceType: 'VALUE',
        replacementValue: 'ss',
        replaceValues: ['address1']
      };
      expect(filterMappers.mapFilterExcludeValues(values, 'TEXT')).to.eql({
        type: 'Value',
        valuesList: ['address1'],
        dataType: 'TEXT'
      });
    });
  });
});
