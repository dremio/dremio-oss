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
import { Table } from 'fixed-data-table-2';
import { AccelerationGrid } from './AccelerationGrid';

const context = {
  context: {
    reflectionSaveErrors: Immutable.fromJS({}),
    lostFieldsByReflection: {}
  }
};

describe('AccelerationGrid', () => {
  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      columns: Immutable.List(),
      shouldShowDistribution: true,
      layoutFields: [
        {
          id:{value:'b'},
          name: {value: 'foo'},
          partitionDistributionStrategy: {value: 'CONSOLIDATED'},
          shouldDelete: {value: false},
          enabled: {value: true}
        }
      ],
      location: {
        state: {}
      },
      activeTab: 'aggregation',
      reflections: Immutable.fromJS({
        a: {id:'a', type: 'AGGREGATION', name:'foo', partitionDistributionStrategy: 'CONSOLIDATED'},
        b: {id:'b', type: 'AGGREGATION', name:'foo', partitionDistributionStrategy: 'STRIPED'},
        c: {id:'c', type: 'RAW', name:'foo', partitionDistributionStrategy: 'CONSOLIDATED'}
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
    wrapper = shallow(<AccelerationGrid {...commonProps}/>, context);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<AccelerationGrid {...minimalProps}/>, context);
    expect(wrapper).to.have.length(1);
  });

  it.skip('should correct render Columns and cells', () => {
    wrapper = mount(<AccelerationGrid {...commonProps}/>, context);
    expect(wrapper.find(Table)).to.have.length(1);
    expect(wrapper.find('.fixedDataTableRowLayout_body .fixedDataTableCellGroupLayout_cellGroupWrapper')).to.have.length(2);
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

  it('#renderExtraLayoutSettingsModal', () => {
    let result = shallow(instance.renderExtraLayoutSettingsModal(0, 'name'));
    expect(result.find('Modal').props().isOpen).to.equal(false);

    instance.setState({visibleLayoutExtraSettingsIndex: 0});
    result = shallow(instance.renderExtraLayoutSettingsModal(0, 'name'));
    expect(result.find('Modal').props().isOpen).to.equal(true);

    instance.renderExtraLayoutSettingsModal(0, 'name').props.hide();
    result = shallow(instance.renderExtraLayoutSettingsModal(0, 'name'));
    expect(result.find('Modal').props().isOpen).to.equal(false);
  });

  describe('#componentWillReceiveProps()', () => {
    it('resets focusedColumn if activeTab changes', () => {
      instance.focusedColumn = 1;
      instance.componentWillReceiveProps({
        activeTab: 'raw',
        layoutFields: [
            {id: {value: 'c'}}
        ]
      });
      expect(instance.focusedColumn).to.equal(undefined);
    });
    it('sets focusedColumn for newly added columns', () => {
      instance.componentWillReceiveProps({
        activeTab: 'aggregation',
        layoutFields: [
            {id: {value: 'b'}},
            {id: {value: 'c'}}
        ]
      });
      expect(instance.focusedColumn).to.equal(1);
    });
    it('does nothing with focusedColumn for newly removed columns (so no jump)', () => {
      instance.focusedColumn = 1;
      instance.componentWillReceiveProps({
        activeTab: 'aggregation',
        layoutFields: []
      });
      expect(instance.focusedColumn).to.equal(1);
    });
  });
});
