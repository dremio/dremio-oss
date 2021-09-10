/*
 * Copyright (C) 2017-2019 Dremio Corporation
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

import DurationProgress from './DurationProgress';

describe('DurationProgress', () => {
  let minimalProps;
  let commonProps;
  const context = { loggedInUser: {} };
  beforeEach(() => {
    minimalProps = {
      progressPercentage: '90'
    };
    commonProps = {
      ...minimalProps,
      title: 'progress title',
      time: 10,
      timePercentage: '20',
      startsFrom: '10'
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DurationProgress {...minimalProps} />, { context });
    expect(wrapper).to.have.length(1);
  });

  it('should render with commonProps props without exploding', () => {
    const wrapper = shallow(<DurationProgress {...commonProps} />, { context });
    expect(wrapper).to.have.length(1);
  });

  it('should render duration with expected result', () => {
    const props = {
      title: 'test title',
      time: 15,
      timePercentage: '60',
      startsFrom: '27',
      progressPercentage: '40'
    };
    const wrapper = shallow(<DurationProgress {...props} />);
    const element = wrapper.find('[data-qa="duration-breakdown-value"]');
    expect(element.text()).to.equal('15s(60%)');
  });
});
