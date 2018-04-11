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

import { DetailsWizard } from 'components/Wizards/DetailsWizard';

import JoinController from 'components/Wizards/JoinWizard/JoinController';
import JoinHeader from 'pages/ExplorePage/components/JoinTypes/JoinHeader';
import RawHeader from 'components/Wizards/components/RawHeader';
import GroupByController from 'components/Wizards/GroupByWizard/GroupByController';

describe('DetailsWizard', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      detailType: '',
      location: { state: {}, query: {}},
      exploreViewState: Immutable.Map()
    };
    commonProps = {
      ...minimalProps,
      tableData: Immutable.fromJS({columns: []}),
      dataset: Immutable.Map()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DetailsWizard {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render <div>', () => {
    const wrapper = shallow(<DetailsWizard {...commonProps}/>);
    expect(wrapper.type()).to.equal('div');
  });

  it('should render correct child if detailType is changed', () => {
    const props = {
      ...commonProps,
      detailType: 'JOIN'
    };
    const wrapper = shallow(<DetailsWizard {...props}/>);
    expect(wrapper.find(JoinController)).to.have.length(1);
    expect(wrapper.find(JoinHeader)).to.have.length(1);
    wrapper.setProps({ detailType: 'GROUP_BY' });
    expect(wrapper.find(RawHeader)).to.have.length(1);
    expect(wrapper.find(GroupByController)).to.have.length(1);
  });
});
