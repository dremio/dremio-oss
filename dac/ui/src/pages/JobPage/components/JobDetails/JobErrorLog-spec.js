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

import JobErrorLog from './JobErrorLog';

describe('JobErrorLog', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      failureInfo: Immutable.fromJS({
        message: 'my error'
      })
    };
    commonProps = {
      failureInfo: Immutable.fromJS({
        errors: [
          {message: 'my error1'},
          {message: 'my error2'}
        ]
      })
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<JobErrorLog {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render errors', () => {
    const wrapper = shallow(<JobErrorLog {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render nothing', () => {
    const emptylProps = {
      failureInfo: Immutable.fromJS({a: 'a'})
    };
    const wrapper = shallow(<JobErrorLog {...emptylProps}/>);
    expect(wrapper.unrendered).to.be.defined;
  });
});
