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

import * as exploreDecorators from './exploreDecorators';

describe('exploreDecorators', () => {

  describe('getColumnStatus()', function() {

    it('should return DELETION_MARKER status', function() {
      const response = Immutable.fromJS({
        highlightedColumns: ['col1'],
        deletedColumns: [],
        rowDeletionMarkerColumns: ['col1']
      });
      const column = Immutable.fromJS({
        name: 'col1'
      });
      expect(exploreDecorators.getColumnStatus(response, column)).to.be.equal('DELETION_MARKER');
    });

    it('should return HIGHLIGHTED status', function() {
      const response = Immutable.fromJS({
        highlightedColumns: ['col1'],
        deletedColumns: [],
        rowDeletionMarkerColumns: []
      });
      const column = Immutable.fromJS({
        name: 'col1'
      });
      expect(exploreDecorators.getColumnStatus(response, column)).to.be.equal('HIGHLIGHTED');
    });

    it('should return DELETED status', function() {
      const response = Immutable.fromJS({
        highlightedColumns: [],
        deletedColumns: ['col1'],
        rowDeletionMarkerColumns: []
      });
      const column = Immutable.fromJS({
        name: 'col1'
      });
      expect(exploreDecorators.getColumnStatus(response, column)).to.be.equal('DELETED');
    });

    it('should return ORIGINAL status', function() {
      const response = Immutable.fromJS({
        highlightedColumns: [],
        deletedColumns: [],
        rowDeletionMarkerColumns: []
      });
      const column = Immutable.fromJS({
        name: 'col1'
      });
      expect(exploreDecorators.getColumnStatus(response, column)).to.be.equal('ORIGINAL');
    });
  });

  describe('isRowDeleted()', function() {

    it('should return false, when no DELETION_MARKER column', function() {
      const row = Immutable.fromJS({
        row: [
          {v: 'foo'}
        ]
      });
      const columns = Immutable.fromJS([
        {
          index: 0,
          status: 'ORIGINAL'
        }
      ]);
      expect(exploreDecorators.isRowDeleted(row, columns)).to.be.false;
    });

    it('should return false, when no null value in row', function() {
      const row = Immutable.fromJS({
        row: [
          {v: 'foo'}
        ]
      });
      const columns = Immutable.fromJS([
        {
          index: 0,
          status: 'DELETION_MARKER'
        }
      ]);
      expect(exploreDecorators.isRowDeleted(row, columns)).to.be.false;
    });

    it('should return true', function() {
      const row = Immutable.fromJS({
        row: [
          {v: null},
          {v: 'bar'}
        ]
      });
      const columns = Immutable.fromJS([
        {
          index: 0,
          status: 'DELETION_MARKER'
        }
      ]);
      expect(exploreDecorators.isRowDeleted(row, columns)).to.be.true;
    });
  });
});
