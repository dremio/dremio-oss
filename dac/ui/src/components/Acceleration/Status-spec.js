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

import Status from './Status';

describe('Status-spec', () => {
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      reflection: Immutable.fromJS({}),
      style: {}
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<Status {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });

  it('should have alt text as message', () => {
    const props = {...minimalProps, reflection: Immutable.fromJS({
      enabled: false
    })};
    const wrapper = shallow(<Status {...props} />);
    const result = wrapper.find('Art');
    expect(result.props().alt).to.equal('Reflection is disabled.');
  });

  it('should have message for manual reflections with failures', () => {
    const props = {...minimalProps, reflection: Immutable.fromJS({
      enabled: true,
      status: {availability: 'AVAILABLE', refresh: 'MANUAL', failureCount: 2}
    })};
    const wrapper = shallow(<Status {...props} />);
    const result = wrapper.find('Art');
    expect(result.props().alt).to.equal('Reflection can accelerate.\n\n2 refresh job attempts failed, will not reattempt.');
  });

  it('should have message for non-manual reflections with failures', () => {
    const props = {...minimalProps, reflection: Immutable.fromJS({
      enabled: true,
      status: {availability: 'AVAILABLE', refresh: 'SCHEDULED', failureCount: 2}
    })};
    const wrapper = shallow(<Status {...props} />);
    const result = wrapper.find('Art');
    expect(result.props().alt).to.equal('Reflection can accelerate.\n\n2 refresh job attempts failed, will reattempt.');
  });

});
