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
  getImmutableTable,
  getNewDatasetFromState
} from './explore';

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

  describe('getExplorePageDataset', () => {

    describe('getNewDatasetFromState', () => {

      it('new dataset should be initialized', () => {
        const state = {routing: {locationBeforeTransitions: {query: {context: 'a.b'}}}};
        const newDataset = getNewDatasetFromState(state);
        expect(newDataset.get('isNewQuery')).to.equal(true);
        expect(newDataset.get('fullPath').toJS()).to.eql(['tmp', 'UNTITLED']);
        expect(newDataset.get('displayFullPath').toJS()).to.eql(['tmp', 'New Query']);
        expect(newDataset.get('context').toJS()).to.eql(['a', 'b']);
        expect(newDataset.get('sql')).to.equal('');
        expect(newDataset.get('datasetType')).to.equal('VIRTUAL_DATASET');
        expect(newDataset.get('apiLinks').get('self')).to.equal('/dataset/tmp/UNTITLED/new_untitled_sql');
        expect(newDataset.get('needsLoad')).to.equal(false);
      });

      it('Query context is decoded correctly', () => {
        // test data is taken from the bug // DX-12354
        const space = '"   tomer 12# $"';
        const folder = '"_ nested $"';
        const contextInput = `${space}.${folder}`;
        const location = {
          query: {
            context: encodeURIComponent(contextInput) // url parameter should be encoded. See NewQueryButton.getNewQueryHref
          }
        };
        const state = {routing: {locationBeforeTransitions: location}};

        const newDataset = getNewDatasetFromState(state);
        const contextResult = newDataset.get('context').toJS();
        expect(contextResult).to.deep.eql([space, folder]);
      });

    });
  });
});
