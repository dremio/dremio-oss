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
import gridTableMapper from './gridTableMapper';
import tableMockedData from './mocks/gridMapper/table.json';
import tableMockedDataExpected from './mocks/gridMapper/expected.json';

describe('Test grid mappers', () => {
  it('test simple data map', () => {
    expect(gridTableMapper.mapJson({}, tableMockedData, 1, true)).to.eql(tableMockedDataExpected);
  });

  describe('#sortFunctions', () => {
    it('should ignore double quotes in func.name and return сorrectly sorted array', () => {
      const result = {
        funcs: [
          { name: 'AVG'},
          { name: 'MAX'},
          { name: 'STDDEV'},
          { name: 'VAR_POP'},
          { name: '"CORR"'},
          { name: '"LEFT"'},
          { name: 'TO_CHAR'},
          { name: 'TO_TIMESTAMP'},
          { name: 'DATE_TRUNC'}
        ]
      };
      gridTableMapper.sortFunctions(result);
      expect(result).to.eql({
        funcs: [
          { name: 'AVG'},
          { name: '"CORR"'},
          { name: 'DATE_TRUNC'},
          { name: '"LEFT"'},
          { name: 'MAX'},
          { name: 'STDDEV'},
          { name: 'TO_CHAR'},
          { name: 'TO_TIMESTAMP'},
          { name: 'VAR_POP'}
        ]
      });
    });
  });
});
