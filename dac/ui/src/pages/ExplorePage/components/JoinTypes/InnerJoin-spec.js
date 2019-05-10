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

import Select from '@app/components/Fields/Select';
import JoinColumnMenu from 'pages/ExplorePage/components/JoinTypes/components/JoinColumnMenu';
import JoinDragArea from 'pages/ExplorePage/components/JoinTypes/components/JoinDragArea';

import { InnerJoin } from './InnerJoin';

describe('InnerJoin', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      columnsInDragArea: Immutable.List(),
      leftColumns: Immutable.fromJS([{
        name: 'foo',
        type: 'INTEGER'
      }]),
      rightColumns: Immutable.fromJS([{
        name: 'baz',
        type: 'INTEGER'
      }]),
      removeColumn: () => {},
      addColumnToInnerJoin: () => {},
      stopDrag: () => {},
      onDragStart: () => {},
      handleDrop: () => {},
      addEmptyColumnToInnerJoin: () => {},
      dragType: 'default',
      fields: {
        joinType: { value: 'Left Outer' }
      }
    };
    commonProps = {
      ...minimalProps
    };
    wrapper = shallow(<InnerJoin {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<InnerJoin {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('#receiveProps', () => {
    it('should update disabledColumnNames only when columnsInDragArea changed', () => {
      const oldLeftDisabledColumnNames = instance.leftDisabledColumnNames;
      const oldRightDisabledColumnNames = instance.leftDisabledColumnNames;

      instance.receiveProps(commonProps, instance.props);
      expect(instance.leftDisabledColumnNames).to.equal(oldLeftDisabledColumnNames);
      expect(instance.rightDisabledColumnNames).to.equal(oldRightDisabledColumnNames);

      instance.receiveProps({...commonProps, columnsInDragArea: Immutable.fromJS([{
        default: { name: 'foo'}
      }, {custom: { name: 'baz'}}])}, commonProps);
      expect(instance.leftDisabledColumnNames).to.not.equal(oldLeftDisabledColumnNames);
      expect(instance.rightDisabledColumnNames).to.not.equal(oldRightDisabledColumnNames);
    });
  });
  describe('#getDisabledColumnNames', () => {
    it('should return columns which is already in the drag area or with unsupported type', () => {
      const props = {
        leftColumns: Immutable.fromJS([
          {
            name: 'foo',
            type: 'INTEGER'
          },
          {
            name: 'bar',
            type: 'MAP'
          },
          {
            name: 'baz',
            type: 'FLOAT'
          },
          {
            name: 'bav',
            type: 'LIST'
          }
        ]),
        columnsInDragArea: Immutable.fromJS([
          {
            default: { name: 'foo' },
            custom: []
          }
        ])
      };
      const disabledColumns = instance.getDisabledColumnNames(props, true);
      expect(disabledColumns.toJS()).to.eql(['foo', 'bar', 'bav']);
    });
  });

  it('should check items Select', () => {
    const items = [
      {
        label: 'Inner',
        des: 'Only matching records',
        icon: 'JoinInner',
        value: 'Inner'
      },
      {
        label: 'Left Outer',
        des: 'All records from left, matching records from right',
        icon: 'JoinLeft',
        value: 'LeftOuter'
      },
      {
        label: 'Right Outer',
        des: 'All records from right, matching records from left',
        icon: 'JoinRight',
        value: 'RightOuter'
      },
      {
        label: 'Full Outer',
        des: 'All records from both',
        icon: 'JoinFull',
        value: 'FullOuter'
      }
    ];
    expect(instance.items).to.eql(items);
  });

  it('should renders inner-join, Select, JoinColumnMenu', () => {
    expect(wrapper.find('.inner-join')).to.have.length(1);
    expect(wrapper.find(Select)).to.have.length(1);
    expect(wrapper.find(JoinColumnMenu)).to.have.length(2);
  });

  it('should render one JoinDragArea if columnsInDragArea have not size', () => {
    expect(wrapper.find(JoinDragArea)).to.have.length(2);
  });

  it('should render one JoinDragArea if columnsInDragArea have size', () => {
    wrapper = shallow(<InnerJoin {...minimalProps} columnsInDragArea={Immutable.fromJS([{a: 1}])}/>);
    expect(wrapper.find(JoinDragArea)).to.have.length(1);
  });
});
