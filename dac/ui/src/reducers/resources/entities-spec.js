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

import {CONVERT_DATASET_TO_FOLDER_SUCCESS} from 'actions/home';
import entities, { cacheConfigs, evictOldEntities } from './entities';

describe('entities', () => {

  const initialState =  Immutable.Map({
    folder: Immutable.fromJS({
      fooId: {
        id: 'fooId',
        queryable: true
      }
    }),
    file: Immutable.fromJS({
      fooFileId: {
        id: 'fooFileId'
      }
    }),
    tableData: Immutable.OrderedMap()
  });

  const payload = Immutable.fromJS({
    entities: {
      space: {
        spaceId: {name: 'foo'}
      }
    }
  });

  it('returns unaltered state by default', () => {
    const result = entities(initialState, {type: 'bla'});
    expect(result).to.equal(initialState);
  });

  it('should do nothing when meta.ignoreEntities is set', () => {
    const result = entities(initialState, {
      type: 'foo',
      meta: { ignoreEntities: true },
      payload
    });
    expect(result).to.equal(initialState);
  });

  it('should merge payload.entities', () => {
    const result = entities(initialState, {
      type: 'foo',
      payload
    });
    expect(result.getIn(['space', 'spaceId', 'name'])).to.eql('foo');
  });

  it('should apply entity reducers', () => {
    const result = entities(initialState, {
      type: CONVERT_DATASET_TO_FOLDER_SUCCESS,
      meta: {folderId: 'fooId'}
    });
    expect(result.getIn(['folder', 'fooId', 'queryable'])).to.be.false;
  });

  it('should both apply entity reducers and merge payload', () => {
    const result = entities(initialState, {
      type: CONVERT_DATASET_TO_FOLDER_SUCCESS,
      payload,
      meta: {folderId: 'fooId'}
    });
    expect(result.getIn(['folder', 'fooId', 'queryable'])).to.be.false;
    expect(result.getIn(['space', 'spaceId', 'name'])).to.eql('foo');
  });

  describe('mergeEntities', () => {
    it('should merge payload when action.meta.mergeEntities is true', () => {
      const result = entities(initialState, {
        type: 'foo',
        payload: Immutable.fromJS({
          entities: {
            folder: {
              fooId: {
                newAttribute: true
              }
            }
          }
        }),
        meta: { mergeEntities: true }
      });
      expect(result.getIn(['folder', 'fooId'])).to.be.eql(Immutable.fromJS({
        id: 'fooId',
        newAttribute: true,
        queryable: true
      }));
    });

    it('should replace entity with payload when action.meta.mergeEntities is false or not defined', () => {
      const action = {
        type: 'foo',
        payload: Immutable.fromJS({
          entities: {
            folder: {
              fooId: {
                newAttribute: true
              }
            }
          }
        }),
        meta: { mergeEntities: false}
      };
      const result = entities(initialState, action);
      expect(result.getIn(['folder', 'fooId'])).to.be.eql(Immutable.fromJS({
        newAttribute: true
      }));

      delete action.meta.mergeEntities;
      const result2 = entities(initialState, action);
      expect(result2.getIn(['folder', 'fooId'])).to.be.eql(Immutable.fromJS({
        newAttribute: true
      }));
    });
  });

  it('should handle entityRemovePaths', () => {
    const result = entities(initialState, {
      meta: {entityRemovePaths: [['folder', 'fooId'], ['file', 'fooFileId']]}
    });
    expect(result.getIn(['folder', 'fooId'])).to.be.undefined;
    expect(result.getIn(['file', 'fooFileId'])).to.be.undefined;
  });

  it('should handle entityClears', () => {
    const result = entities(initialState, {
      meta: {entityClears: ['folder', 'file']}
    });
    expect(result.getIn(['folder', 'fooId'])).to.be.undefined;
    expect(result.getIn(['file', 'fooFileId'])).to.be.undefined;
  });

  describe('evictOldEntities()', () => {
    const cache = Immutable.OrderedMap([['a', 1], ['b', 2], ['c', 3]]);

    it('should do nothing if cache not at max', () => {
      const result = evictOldEntities(cache, cache.size);
      expect(result.size).to.equal(cache.size);
    });

    it('should remove old entries if size > max', () => {
      const result = evictOldEntities(cache, 1);
      expect(result.size).to.equal(1);
      expect(result.get('c')).to.equal(cache.get('c'));
    });
  });

  describe('evicting old values', () => {
    function getTableDataAction(id) {
      return {
        type: 'foo',
        payload: Immutable.fromJS({
          entities: {
            tableData: {
              [id]: {id, name: 'foo'}
            }
          }
        })
      };
    }

    it('should update values as normal', () => {
      const result = entities(initialState, getTableDataAction('id1'));
      expect(result.getIn(['tableData', 'id1', 'name'])).to.eql('foo');
    });

    it('should remove oldest values', () => {
      let result = initialState;
      const cacheMax = cacheConfigs.tableData.max;
      for (let i = 0; i < cacheMax; i++) {
        result = entities(result, getTableDataAction(`id${i}`));
      }
      expect(result.get('tableData').size).to.equal(cacheMax);

      result = entities(result, getTableDataAction('oneMore'));
      expect(result.get('tableData').size).to.equal(cacheMax);

      expect(result.getIn(['tableData', 'id0'])).to.be.undefined;
    });
  });
});
