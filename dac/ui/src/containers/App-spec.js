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
import { shallow } from 'enzyme';
import Immutable from 'immutable';
import configureMockStore from 'redux-mock-store';
import thunk from 'redux-thunk';

import App from './App';

const middlewares = [ thunk ];
const mockStore = configureMockStore(middlewares);
const store = mockStore({home: { config: Immutable.Map({}) }});

describe('App-spec', () => {
  describe('render', () => {
    it('should render empty app', () => {
      const wrapper = shallow(<App store={store}/>);
      expect(
        wrapper.children().first().text()
      ).to.eql('');
    });
  });
});
