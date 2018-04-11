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

import DragColumnMenu from  './DragColumnMenu';

describe('DragColumnMenu', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      items: Immutable.List(),
      disabledColumnNames: Immutable.Set(),
      namesOfColumnsInDragArea: [],
      name: '',
      dragType: 'explore'
    };
    commonProps = {
      ...minimalProps
    };
    wrapper = shallow(<DragColumnMenu {...commonProps}/>);
    instance = wrapper.instance();
  });

  describe('#render()', function() {
    it('should render with minimal props without exploding', () => {
      wrapper = shallow(<DragColumnMenu {...minimalProps}/>);
      expect(wrapper).to.have.length(1);
    });
  });

  describe('#componentWillUpdate()', () => {
    it('should update this.filteredSortedColumns if filter, columns or disabledColumnNames has changed', () => {
      sinon.stub(instance, 'updateColumns');
      instance.componentWillUpdate(instance.props, instance.state);
      expect(instance.updateColumns).to.not.be.called;
      instance.componentWillUpdate(instance.props, {filter: 'someFilter'});
      expect(instance.updateColumns).to.have.callCount(1);
      instance.componentWillUpdate({...instance.props, items: Immutable.List([{}])}, instance.state);
      expect(instance.updateColumns).to.have.callCount(2);
      instance.componentWillUpdate({...instance.props, disabledColumnNames: Immutable.List(['foo'])}, instance.state);
      expect(instance.updateColumns).to.have.callCount(3);
    });
  });

  describe('#sortColumns()', () => {
    const columns = Immutable.fromJS([
      {index: 0, name: 'foo'},
      {index: 1, name: 'bar'},
      {index: 2, name: 'baz'}
    ]);
    it('should return original order if all are disabled or everything is not disabled', () => {
      const allColumnNames = Immutable.Set(['foo', 'bar', 'baz']);
      expect(DragColumnMenu.sortColumns(columns, Immutable.Set())).to.eql(columns);
      expect(DragColumnMenu.sortColumns(columns, allColumnNames)).to.eql(columns);
    });

    it('should return disabled columns last', () => {
      expect(
        DragColumnMenu.sortColumns(columns, Immutable.Set(['bar'])).map((col) => col.get('name'))
      ).to.eql(Immutable.List(['foo', 'baz', 'bar']));
    });
  });

  describe('#filterColumns()', () => {
    it('should filter out columns that do not contain the filter (case insensitive) in their name', () => {
      const columns = Immutable.fromJS([
        {index: 0, name: 'abc'},
        {index: 1, name: 'defABcGHi'},
        {index: 2, name: 'abcDEFghi'}
      ]);
      expect(instance.filterColumns('abc', columns)).to.eql(columns);
      expect(instance.filterColumns('def', columns)).to.eql(columns.slice(1));
    });
  });
});
