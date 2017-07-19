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
import { shallow } from 'enzyme';
import Immutable from 'immutable';
import { Column, Table } from 'fixed-data-table-2';
import { AccelerationGrid } from './AccelerationGrid';

describe('AccelerationGrid', () => {
  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      columns: Immutable.List(),
      shouldShowDistribution: true,
      layoutFields: [{id:{id:{value:'b'}}}],
      location: {
        state: {}
      },
      activeTab: 'aggregation',
      acceleration: Immutable.fromJS({
        aggregationLayouts: {
          // WARNING: this might not be exactly accurate - but it's enough for the test
          layoutList: [
            {id:{id:'a'}}, // deleted
            {id:{id:'b'}}
          ],
          enabled: true
        },
        rawLayouts: {
          // WARNING: this might not be exactly accurate - but it's enough for the test
          layoutList: [
            {id:{id:'c'}}
          ],
          enabled: true
        }
      })
    };
    commonProps = {
      ...minimalProps,
      renderBodyCell: sinon.spy(),
      columns: Immutable.List([
        { name: 'columnA', type: 'TEXT' },
        { name: 'columnB', type: 'TEXT' }
      ])
    };
    wrapper = shallow(<AccelerationGrid {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<AccelerationGrid {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should correct render Columns and cells', () => {
    expect(wrapper.find(Table)).to.have.length(1);
    expect(wrapper.find(Column)).to.have.length(2);
  });

  describe('#renderSubCellHeaders', () => {
    it('should render Dimension and Measure when activeTab != raw and Display when activeTab == raw', () => {
      let result = shallow(instance.renderSubCellHeaders());
      expect(result.find('div')).to.have.length(6);
      expect(result.find('div').at(1).text()).to.eql('Dimension');
      expect(result.find('div').at(2).text()).to.eql('Measure');

      wrapper.setProps({activeTab: 'raw'});
      result = shallow(instance.renderSubCellHeaders());
      expect(result.find('div')).to.have.length(5);
      expect(result.find('div').at(1).text()).to.eql('Display');
    });

    it('should only render Distribution when props.shouldShowDistribution', () => {
      let result = shallow(instance.renderSubCellHeaders());
      expect(result.find('div')).to.have.length(6);
      expect(result.find('div').at(5).text()).to.eql('Distribution');

      wrapper.setProps({shouldShowDistribution: false});
      result = shallow(instance.renderSubCellHeaders());
      expect(result.find('div')).to.have.length(5);
      expect(result.find('div').at(4).text()).to.eql('Partition');
    });
  });

  describe('#findLayoutData()', () => {
    it('aggregation', () => {
      expect(instance.findLayoutData(0).toJS()).to.eql({id:{id:'b'}});
    });
    it('raw', () => {
      wrapper.setProps({
        activeTab: 'raw',
        layoutFields: [
          {id: {id: {value: 'c'}}}
        ]
      });
      expect(instance.findLayoutData(0).toJS()).to.eql({id:{id:'c'}});
    });
  });
});
