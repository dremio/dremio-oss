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

import { ExploreTableColumnFilter } from './ExploreTableColumnFilter';

describe('ExploreTableColumnFilter', () => {

  let minimalProps;

  beforeEach(() => {
    minimalProps = {
      dataset: Immutable.fromJS({}),
      columnFilter: '',
      updateColumnFilter: () => {},
      columnCount: 0,
      filteredColumnCount: 0,
      location: {pathname: ''}
    };
  });

  describe('render', () => {

    it('should render with minimal props without exploding', () => {
      const wrapper = shallow(<ExploreTableColumnFilter {...minimalProps}/>);
      expect(wrapper).to.have.length(1);
      expect(wrapper.find('div[data-qa=\'columnFilter\']')).to.have.length(1);
    });

    it('should hide column filter count w/o filter', () => {
      const wrapper = shallow(<ExploreTableColumnFilter {...minimalProps}/>);
      expect(wrapper.find('div[data-qa=\'columnFilterStats\']')).to.have.length(1);
      expect(wrapper.find('span[data-qa=\'columnFilterCount\']')).to.have.length(0);
    });

    it('should render column full filter stats with filter', () => {
      const props = {
        ...minimalProps,
        columnFilter: 'test',
        columnCount: 4,
        filteredColumnCount: 2
      };
      const wrapper = shallow(<ExploreTableColumnFilter {...props}/>);
      expect(wrapper.find('span[data-qa=\'columnFilterCount\']')).to.have.length(1);
      expect(wrapper.find('span[data-qa=\'columnFilterCount\']').text()).to.eql('2 of ');
    });
  });

});
