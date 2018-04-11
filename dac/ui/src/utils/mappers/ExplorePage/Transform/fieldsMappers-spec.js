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
import fieldsMappers from './fieldsMappers.js';

describe('fieldsMappers', () => {

  describe('getCommonValues', () => {
    it('should return commonValues', () => {
      const result = fieldsMappers.getCommonValues({
        newFieldName: 'newFieldName',
        dropSourceField: true
      }, Immutable.Map({columnName: 'columnName'}));
      expect(result).to.eql({
        type: 'field',
        newColumnName: 'newFieldName',
        sourceColumnName: 'columnName',
        dropSourceColumn: true
      });
    });
  });

  describe('getReplaceExact', () => {
    it('should return correct values', () => {
      const values = {
        newFieldName: 'age',
        dropSourceField: false,
        activeCard: 0,
        replaceNull:false,
        replaceType: 'VALUE',
        replacementValue: '1',
        replaceValues: ['16']
      };
      expect(fieldsMappers.getReplaceExact(values, 'STRING')).to.eql({
        replaceNull: false,
        replacementValue: '1',
        replacedValuesList: ['16'],
        replacementType: 'STRING',
        replaceType: undefined
      });
    });
  });

  describe('getReplaceRange', () => {
    it('should return correct values', () => {
      const values = {
        newFieldName: 'age',
        dropSourceField: false,
        lowerBound: 2,
        upperBound: 4,
        keepNull: false,
        lowerBoundInclusive: true,
        upperBoundInclusive: false,
        replaceType: 'VALUE',
        replacementValue: '1'
      };
      expect(fieldsMappers.getReplaceRange(values, 'INTEGER')).to.eql({
        lowerBound: 2,
        upperBound: 4,
        keepNull: false,
        lowerBoundInclusive: true,
        upperBoundInclusive: false,
        replacementType: 'INTEGER',
        replacementValue: '1'
      });
    });

    it('should return null for empty bounds', () => {
      const values = {
        newFieldName: 'age',
        dropSourceField: false,
        lowerBound: '',
        upperBound: '',
        keepNull: false,
        lowerBoundInclusive: true,
        upperBoundInclusive: false,
        replaceType: 'VALUE',
        replacementValue: '1'
      };
      expect(fieldsMappers.getReplaceRange(values, 'INTEGER')).to.eql({
        lowerBound: null,
        upperBound: null,
        keepNull: false,
        lowerBoundInclusive: true,
        upperBoundInclusive: false,
        replacementType: 'INTEGER',
        replacementValue: '1'
      });
    });
  });

  describe('getReplaceValues', () => {
    it('should return correct values', () => {
      const values = {
        newFieldName: 'a2',
        dropSourceField: false,
        activeCard: 0,
        replaceSelectionType: 'VALUE',
        replaceType: 'VALUE',
        replacementValue: 'ss',
        replaceValues: ['address1']
      };
      expect(fieldsMappers.getReplaceValues(values, 'INTEGER')).to.eql({
        replaceNull: false,
        replacementValue: 'ss',
        replacementType: 'INTEGER',
        replacedValuesList: ['address1']
      });
    });
  });

  describe('getSplitPosition', () => {
    const values = {
      maxFields: 10,
      index: 1
    };

    it('should return maxFields for position All', () => {
      expect(fieldsMappers.getSplitPosition({
        position: 'All',
        ...values
      })).to.eql({
        position: 'ALL',
        maxFields: 10
      });
    });

    it('should return index for position Index', () => {
      expect(fieldsMappers.getSplitPosition({
        position: 'Index',
        ...values
      })).to.eql({
        position: 'INDEX',
        index: 1
      });
    });

    it('should only return position otherwise', () => {
      expect(fieldsMappers.getSplitPosition({
        position: 'First',
        ...values
      })).to.eql({
        position: 'FIRST'
      });
    });
  });
});
