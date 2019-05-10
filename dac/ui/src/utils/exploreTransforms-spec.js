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
import exploreTransforms from './exploreTransforms';

describe('exploreTransforms', () => {
  it('should drop a column and a data for a column', () => {
    const originalData = Immutable.fromJS({
      columns: [
        {
          name: 'col1'
        },
        {
          name: 'col2'
        }
      ],
      rows: [{
        row: ['col1 value', 'col2 value']
      }]
    });
    expect(exploreTransforms.dropColumn({ name: 'col1', table: originalData }).toJS()).to.be.eql(
      {
        columns: [
          {
            name: 'col2'
          }
        ],
        rows: [{ row: ['col2 value'] }]
      });
  });
  it('should work fine if there is no rows', () => {
    const originalData = Immutable.fromJS({
      columns: [
        {
          name: 'col1'
        },
        {
          name: 'col2'
        }
      ]
    });
    expect(exploreTransforms.dropColumn({ name: 'col1', table: originalData }).toJS()).to.be.eql(
      {
        columns: [
          {
            name: 'col2'
          }
        ]
      });
  });
});
