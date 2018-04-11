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

import { CUSTOM_JOIN, RECOMMENDED_JOIN } from 'constants/explorePage/joinTabs';
import ExploreTableController from './ExploreTableController';

import { JoinTables } from './JoinTables';

describe('JoinTables', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = {
      pageType: 'default',
      dataset: Immutable.fromJS({
        datasetVersion: 'someDatasetVersion'
      }),
      exploreViewState: Immutable.Map(),
      location: {
        query: {},
        state: {}
      },
      sqlSize: 111,
      rightTreeVisible: false,
      accessEntity: sinon.spy(),
      joinDataset: '',
      joinTab: CUSTOM_JOIN,
      joinTableData: Immutable.fromJS({columns: Immutable.Map()}),
      joinViewState: Immutable.Map()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<JoinTables {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render ExploreTableController', () => {
    const wrapper = shallow(<JoinTables {...commonProps}/>);
    expect(wrapper.find(ExploreTableController)).to.have.length(1);
  });

  describe('#componentWillReceiveProps', () => {
    it('should call props.accessEntity only when location.state.joinVersion changes', () => {
      const props = {...commonProps, joinVersion: 'version1', joinStep: 2};
      const wrapper = shallow(<JoinTables {...props}/>);
      expect(commonProps.accessEntity).to.not.be.called;

      wrapper.setProps({joinVersion: 'version2'});
      expect(commonProps.accessEntity).to.be.calledWith('tableData', commonProps.dataset.get('datasetVersion'));
    });
  });

  describe('#shouldRenderSecondTable', () => {
    it('should return true for Custom Join and step 1', () => {
      const props = {...commonProps, joinVersion: 'version1', joinStep: 1};
      const instance = shallow(<JoinTables {...props}/>).instance();
      expect(instance.shouldRenderSecondTable()).to.be.equals(true);
    });

    it('should return false for Custom Join and step 2', () => {
      const props = {...commonProps, joinVersion: 'version1', joinStep: 2};
      const instance = shallow(<JoinTables {...props}/>).instance();
      expect(instance.shouldRenderSecondTable()).to.be.equals(false);
    });

    it('should return false for Recommended Join', () => {
      const props = {...commonProps, joinVersion: 'version1', joinStep: 1, joinTab: RECOMMENDED_JOIN};
      const instance = shallow(<JoinTables {...props}/>).instance();
      expect(instance.shouldRenderSecondTable()).to.be.equals(false);
    });
  });
});
