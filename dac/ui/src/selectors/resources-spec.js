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

import { CREATED_SOURCE_NAME } from 'reducers/resources/sourceList';

import entities from './mocks/entities.json';
import space from './mocks/spaceList.json';
import source from './mocks/sourceList.json';

import spacesAfterSort1 from './mocks/spacesAfterSort1.json';
import spacesAfterSort10 from './mocks/spacesAfterSort10.json';
import sourcesAfterSort1 from './mocks/sourcesAfterSort1.json';
import sourcesAfterSort10 from './mocks/sourcesAfterSort10.json';

import {
  getSortedSpaces,
  getSortedSources,
  getCreatedSource,
  getEntity,
  getViewState
} from './resources';


describe('datasets selectors', () => {
  const state = {
    resources: {
      entities: Immutable.fromJS(entities),
      view: {
        source: Immutable.fromJS(source),
        space: Immutable.fromJS(space)
      }
    }
  };

  describe('getSortedSpaces selector', () => {
    xit('should return tree', () => {
      expect(getSortedSpaces(state.view.space).toJS()).to.eql({spacesAfterSort1});
      const setPinState = {
        resources: {
          entities: Immutable.fromJS(entities),
          view: {
            source: Immutable.fromJS(source),
            space: Immutable.fromJS(space).set('xcxcx').set('isActivePin', true)
          }
        }
      };
      expect(getSortedSpaces(setPinState).toJS()).to.eql(spacesAfterSort10);
    });
  });
  describe('getSortedSources selector', () => {
    xit('should return tree', () => {
      expect(getSortedSources(state).toJS()).to.eql(sourcesAfterSort1);
      const setPinState = {
        resources: {
          sources: state.resources.sources.setIn(['sources', 'LocalFS2', 'isActivePin'], true)
        }
      };
      expect(getSortedSources(setPinState).toJS()).to.eql(sourcesAfterSort10);
    });
  });
  describe('getCreatedSource selector', () => {
    xit('should return tree', () => {
      expect(getCreatedSource(state)).to.eql(undefined);
      const createdSourceState = {
        resources: {
          sources: state.resources.sources.setIn([CREATED_SOURCE_NAME], Immutable.Map({name: 1}))
        }
      };
      expect(getCreatedSource(createdSourceState).toJS()).to.eql({name: 1});
    });
  });
});

describe('entity selectors', () => {
  const homeId = 'uuid';
  const homeSpace = Immutable.fromJS({
    id: homeId,
    resourcePath: '/home/@foo'
  });
  const state = {
    account: Immutable.fromJS({
      user: {userName: 'foo'}
    }),
    resources: {
      entities: Immutable.fromJS({
        home: {
          [homeId]: homeSpace
        }
      })
    }
  };
  describe('getEntity', () => {
    it('should return entity', () => {
      expect(getEntity(state, homeId, 'home')).to.eql(state.resources.entities.getIn(['home', homeId]));
    });
  });

  describe('getViewState', () => {
    it('should return view state if available', () => {
      const ret = getViewState({resources: {view: new Immutable.Map({id: 'foo'})}}, 'id');
      expect(ret).to.equal('foo');
    });

    it('should return (cached) fallback', () => {
      expect(getViewState({}, 'foo')).to.equal(getViewState({}, 'foo'));
      expect(getViewState({}, 'foo')).to.not.equal(getViewState({}, 'bar'));
    });
  });

});
