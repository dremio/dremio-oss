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
import DragMeasureColumn from './DragMeasureColumn';

describe('DragMeasureColumn', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      dragType: 'dimension',
      field: {
        column: {
          value: '',
          onChange: sinon.spy()
        },
        measure: {
          value: '',
          onChange: sinon.spy()
        }
      }
    };
    commonProps = {
      ...minimalProps,
      dragType: 'groupBy',
      item: Immutable.Map({ id: 1, empty: true }),
      field: {
        measure: {
          value: 'Sum'
        },
        column: {
          value: 2
        }
      },
      allColumns: Immutable.fromJS([
        { name: 'item1', type: 'FLOAT' },
        { name: 'item2', type: 'DATE' },
        { name: 'item3' }
      ])
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DragMeasureColumn {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should render Select, DragAreaColumn', () => {
    const wrapper = shallow(<DragMeasureColumn {...commonProps}/>);
    expect(wrapper.find('Select')).to.have.length(1);
    expect(wrapper.find('DragAreaColumn')).to.have.length(1);
  });

  describe('#canSelectColumn', () => {
    let instance;
    beforeEach(() => {
      instance = shallow(<DragMeasureColumn {...commonProps}/>).instance();
    });
    it('should return true if type is valid for measure', () => {
      expect(instance.canSelectColumn('item1')).to.be.true;
    });
    it('should return false if type is invalid for measure', () => {
      expect(instance.canSelectColumn('item2')).to.be.false;
    });
  });
});
