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

import DragAreaColumn from  './DragAreaColumn';

describe('DragAreaColumn', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      field: {
        value: '',
        onChange: sinon.spy()
      },
      removeColumn: sinon.spy(),
      dragOrigin: 'MAP',
      id: '1',
      dragType: ''
    };
    commonProps = {
      ...minimalProps,
      allColumns: Immutable.fromJS(
        [
          {name: 'col1', type: 'TEXT'},
          {name: 'col2', type: 'TEXT'}
        ]
      )
    };
  });

  describe('#render()', function() {

    it('should render with minimal props without exploding', () => {
      const wrapper = shallow(<DragAreaColumn {...minimalProps}/>);
      expect(wrapper).to.have.length(1);
    });
  });

  describe('#selectColumn', () => {
    it('should call field.onChange with column name', () => {
      const closeDD = sinon.stub();
      const instance = shallow(<DragAreaColumn {...commonProps}/>).instance();
      instance.selectColumn({columnName: 'col1'}, closeDD);
      expect(closeDD).to.be.called;
      expect(commonProps.field.onChange).to.have.been.calledWith('col1');
    });

    it('should do nothing if canSelectColumn returns false', () => {
      const instance = shallow(<DragAreaColumn {...commonProps} canSelectColumn={() => false}/>).instance();
      instance.selectColumn({columnName: 'col1'});
      expect(commonProps.field.onChange).to.not.have.been.called;
    });
  });

  describe('#filterColumns', () => {
    it('should filter columns based on pattern', () => {
      const wrapper = shallow(<DragAreaColumn {...commonProps}/>);
      const instance = wrapper.instance();

      wrapper.setState({
        pattern: 'COL1'
      });
      let filteredColumns = instance.filterColumns();
      expect(filteredColumns).to.have.size(1);
      expect(filteredColumns).to.eql(Immutable.fromJS([{name: 'col1', type: 'TEXT'}]));

      wrapper.setProps({
        allColumns: Immutable.fromJS(
          [
            {name: 'FOO', type: 'TEXT'},
            {name: 'BAZ', type: 'TEXT'}
          ]
        )
      });
      wrapper.setState({
        pattern: 'foo'
      });
      filteredColumns = instance.filterColumns();
      expect(filteredColumns).to.have.size(1);
      expect(filteredColumns).to.eql(Immutable.fromJS([{name: 'FOO', type: 'TEXT'}]));
    });
  });

  describe('#renderAllColumns()', function() {

    it('should render a column', function() {
      const instance = shallow(<DragAreaColumn {...commonProps}/>).instance();
      expect(instance.renderAllColumns().toArray()).to.have.length(2);
    });

    it('should only render column that match pattern', function() {
      const instance = shallow(<DragAreaColumn {...commonProps}/>).instance();
      instance.setState({pattern: 'col1'});
      expect(instance.renderAllColumns().toArray()).to.have.length(1);
    });
  });

});
