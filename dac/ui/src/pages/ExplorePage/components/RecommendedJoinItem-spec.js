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

import RecommendedJoinItem from './RecommendedJoinItem';

describe('RecommendedJoinItem', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      selectJoin: sinon.spy(),
      recommendation: Immutable.fromJS({
        joinType: 'INNER',
        matchingKeys: {
          age: 'user'
        },
        rightTableFullPathList: ['Prod-Sample', 'ds1']
      })
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<RecommendedJoinItem {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('#goToCustomTab', () => {
    it('should navigate to custom join tab', () => {
      const event = { stopPropagation: sinon.spy() };
      shallow(<RecommendedJoinItem {...commonProps}/>).instance().goToCustomTab(event);
      expect(commonProps.selectJoin).to.be.calledWith(commonProps.recommendation, true);
    });
  });

  describe('#selectRecommendation', () => {
    it('should activate clicked item', () => {
      const event = { stopPropagation: sinon.spy() };
      shallow(<RecommendedJoinItem {...commonProps}/>, {context}).instance().selectRecommendation(event);
      expect(commonProps.selectJoin).to.be.calledWith(commonProps.recommendation, false);
    });
  });
});
