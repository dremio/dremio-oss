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
import transformRules from './transformRules.js';

describe('transformRules', () => {

  describe('mapPositionRule', () => {
    it('should return correct values', () => {
      const result = transformRules.mapPositionRule({
        type: 'position',
        position: {
          startIndex: {value: 1, direction: 'FROM_THE_START'},
          endIndex: {value: 2, direction: 'FROM_THE_START'}
        }
      });
      expect(result).to.eql({
        type: 'position',
        position: {
          startIndex: {
            value: 1,
            direction: 'FROM_THE_START'
          },
          endIndex: {
            value: 2,
            direction: 'FROM_THE_START'
          }
        }
      });
    });
  });

  describe('mapPatternRule', () => {
    it('should return correct values', () => {
      const result = transformRules.mapPatternRule({
        type: 'pattern',
        ignoreCase: true,
        pattern: {
          pattern: 'ss',
          value: {index: 1, value: 'INDEX'}
        }
      });
      expect(result).to.eql({
        type: 'pattern',
        pattern: {
          ignoreCase: true,
          pattern: 'ss',
          index: 1,
          indexType: 'INDEX'
        }
      });
    });
  });
});
