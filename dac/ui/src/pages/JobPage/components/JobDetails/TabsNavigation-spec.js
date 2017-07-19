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
import { shallow, mount } from 'enzyme';
import TabsNavigation from './TabsNavigation';
describe('TabsNavigation', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      activeTab: 'overview',
      changeTab: sinon.spy()
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<TabsNavigation {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render Overview tab as active by default', () => {
    const wrapper = mount(<TabsNavigation {...minimalProps}/>);
    const activeTab = wrapper.find('.tab-link.active');
    expect(activeTab).to.have.length(1);
    expect(activeTab.text()).to.be.eql('Overview');
  });

  it('should render 3 tabs: Overview, Details and Profiles by default', () => {
    const wrapper = mount(<TabsNavigation {...minimalProps}/>);
    const tabs = wrapper.find('TabsNavigationItem');
    const expectedLabels = ['Overview', 'Details', 'Profiles'];
    expect(tabs).to.have.length(3);
    expect(tabs.map(i => i.text())).to.be.eql(expectedLabels);
  });

  it('should render 3 tabs: Overview, Details and Profile when job has single profile', () => {
    const props = {
      ...commonProps,
      attemptDetails: Immutable.fromJS([
        {profileUrl: 'profileUrl'}
      ])
    };
    const wrapper = mount(<TabsNavigation {...props}/>);
    const tabs = wrapper.find('TabsNavigationItem');
    const expectedLabels = ['Overview', 'Details', 'Profile'];
    expect(tabs).to.have.length(3);
    expect(tabs.map(i => i.text())).to.be.eql(expectedLabels);
  });
});
