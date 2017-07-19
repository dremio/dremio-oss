/*
 * Copyright (C) 2017 Dremio Corporation
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

import { getImmutableTable } from './explore';

const emptyTable = Immutable.fromJS({
  columns: [],
  rows: []
});

// TODO should be refactored and updated, here used old mocked data
describe('explore selectors', () => {
  describe('getImmutableTable', () => {
    let entities;

    beforeEach(() => {
      entities = Immutable.fromJS({
        tableData: {
          someVersion: {
            columns: [],
            rows: []
          }
        },
        table: {
          previewVersion: {
            columns: [],
            rows: []
          }
        }
      });
    });

    it('returns empty table for unknown version', () => {
      expect(getImmutableTable({resources: {entities}}, 'unknown', {})).to.eql(emptyTable);
    });

    it('returns tableData for version', () => {
      expect(
        getImmutableTable({resources: {entities}}, 'someVersion', {})
      ).to.eql(entities.getIn(['tableData', 'someVersion']));
    });

    it('returns previewTable for version', () => {
      expect(
        getImmutableTable({resources: {entities}}, 'someVersion', {
          query: {type: 'default'}, state: {previewVersion: 'previewVersion'}
        })
      ).to.eql(entities.getIn(['table', 'previewVersion']));
    });
  });
});
