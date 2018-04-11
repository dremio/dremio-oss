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
import { shallow } from 'enzyme';
import Immutable from 'immutable';
import Quote from './Quote.js';

describe('Test quote component', () => {


  it('should hide stats box for 0/0', () => {
    const props = {
      jobIOData: Immutable.fromJS({
        inputBytes: 0,
        outputBytes: 0
      })
    };
    const wrapper = shallow(<Quote {...props}/>);
    expect(wrapper.instance().render()).to.eql(null);
  });

  it('should hide stats box for {}', () => {
    const props = {
      jobIOData: Immutable.fromJS({})
    };
    const wrapper = shallow(<Quote {...props}/>);
    expect(wrapper.instance().render()).to.eql(null);
  });

});
