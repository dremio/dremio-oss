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

import { RECOMMENDED_JOIN, CUSTOM_JOIN } from 'constants/explorePage/joinTabs';
import { JoinHeader } from './JoinHeader';

describe('JoinHeader', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  beforeEach(() => {
    minimalProps = {
      closeIconHandler: sinon.spy(),
      viewState: Immutable.Map()

    };
    commonProps = {
      hasRecommendations: false,
      isRecommendationsInProgress: false,
      closeIcon: true,
      setJoinTab: sinon.spy(),
      joinTab: CUSTOM_JOIN,
      clearJoinDataset: sinon.spy(),
      ...minimalProps
    };
    wrapper = shallow(<JoinHeader {...commonProps}/>);
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<JoinHeader {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render two tabs', () => {
    expect(wrapper.find('.transform-tab')).to.have.length(2);
  });

  it('#setActiveTab', () => {
    const instance = wrapper.instance();

    instance.setActiveTab(RECOMMENDED_JOIN);
    expect(instance.props.clearJoinDataset).to.have.been.called;
    expect(instance.props.setJoinTab).to.have.been.calledWith(RECOMMENDED_JOIN);
  });
});
