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

import Tabs from './Tabs';

describe('Tabs-spec', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = {
      activeTab: 'activeTabId'
    };
  });

  it('render Tabs', () => {
    const wrapper = shallow(
      <Tabs {...commonProps}>
        <div tabId='otherTabId' className='tab'><span>First tab content</span></div>
        <div tabId='activeTabId' className='tab'><span>Second tab content</span></div>
      </Tabs>
    );
    expect(wrapper.hasClass('tab-wrapper')).to.be.true;
  });

  it('Tab Component content', () => {
    const wrapper = shallow(
      <Tabs {...commonProps}>
        <div tabId='otherTabId' className='tab'><span>First tab content</span></div>
        <div tabId='activeTabId' className='tab'><span>Second tab content</span></div>
      </Tabs>
    );
    expect(wrapper.find('.tab')).to.have.length.of(1);
    expect(wrapper.find('.tab').text()).to.equal('Second tab content');
  });
});
