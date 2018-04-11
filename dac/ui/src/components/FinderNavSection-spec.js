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

import FinderNavSection from './FinderNavSection';

describe('FinderNavSection', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = {
      items: Immutable.fromJS([{id: '1'}, {id: '2'}, {id: '3'}]),
      maxItemsCount: 5,
      toggleActivePin: sinon.spy(),
      isInProgress: false
    };
  });

  it('renders items', () => {
    const wrapper = shallow(<FinderNavSection {...commonProps}/>);
    expect(wrapper.hasClass('holder')).to.be.true;
    expect(wrapper.find('FinderNavItem')).to.have.length(commonProps.items.size);
  });

  it('renders show all when items.size > maxItemsCount', () => {
    let wrapper = shallow(<FinderNavSection {...commonProps}/>);
    expect(wrapper.find('.show-more-btn')).to.have.length(0);

    wrapper = shallow(<FinderNavSection {...commonProps} maxItemsCount={2}/>);
    expect(wrapper.find('.show-more-btn')).to.have.length(1);
    expect(wrapper.find('.show-more-btn').props().children).to.equal('Show All (3) »');
    expect(wrapper.find('FinderNavItem')).to.have.length(2);
  });

  it('without hidden items', () => {
    const wrapper = shallow(<FinderNavSection {...commonProps} maxItemsCount={100} />);
    expect(wrapper.find('.show-more-btn')).have.length(0);
    expect(wrapper.find('FinderNavItem')).have.length(commonProps.items.size);
  });

  it('loading state', () => {
    const wrapper = shallow(<FinderNavSection {...commonProps} isInProgress />);
    expect(wrapper.find('.loader')).have.length(0);
  });
});
