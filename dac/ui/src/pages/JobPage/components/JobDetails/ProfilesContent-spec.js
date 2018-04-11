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
import { shallow, mount } from 'enzyme';
import Immutable from 'immutable';

import ProfilesContent from './ProfilesContent';

describe('ProfilesContent', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      showJobProfile: sinon.spy(),
      jobDetails: Immutable.fromJS({
        attemptDetails: [
          {
            reason: 'Schema Learning',
            profileUrl: '/profiles/schema-learning'
          },
          {
            reason: 'Insufficient Memory',
            profileUrl: '/profiles/insufficient'
          }
        ]
      })
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ProfilesContent {...minimalProps}/>);
    expect(wrapper).to.have.length(1);

    wrapper.setProps({jobDetails: Immutable.Map()});
    expect(wrapper).to.have.length(1);

    wrapper.setProps({jobDetails: Immutable.fromJS({attemptDetails: []})});
    expect(wrapper).to.have.length(1);
  });

  it('should render title', () => {
    const wrapper = shallow(<ProfilesContent {...commonProps}/>);
    expect(wrapper.find('h4').text()).to.eql('Attempts');
  });

  it('should render profile items in reverse order', () => {
    const wrapper = shallow(<ProfilesContent {...commonProps}/>);
    expect(wrapper.find('.profiles').find('div').at(1).text()).to.eql('Attempt 2 (Insufficient Memory)Profile »');
    expect(wrapper.find('.profiles').find('div').at(3).text()).to.eql('Attempt 1 (Schema Learning)Profile »');
  });

  it('should open modal with appropriate job profile when clicked', () => {
    const wrapper = mount(<ProfilesContent {...commonProps}/>);
    const profiles = wrapper.find('.profiles > div');
    profiles.at(0).find('a').simulate('click');
    expect(commonProps.showJobProfile).to.be.calledWith('/profiles/insufficient');
    commonProps.showJobProfile.reset();
    profiles.at(1).find('a').simulate('click');
    expect(commonProps.showJobProfile).to.be.calledWith('/profiles/schema-learning');
  });
});
