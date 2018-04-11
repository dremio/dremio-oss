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
import Mousetrap from 'mousetrap';

import BrowseTable from './BrowseTable';

describe('BrowseTable', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      tableData: Immutable.List([
        { data: { name: { node: 'foo', value: 'foo'}}},
        { data: { name: { node: 'bar', value: 'bar'}}},
        { data: { name: { node: 'baz', value: 'baz'}}}
      ])
    };
    commonProps = {
      ...minimalProps,
      title: 'Browse',
      columns: [
        {title: 'name'}
      ]
    };
    sinon.stub(Mousetrap, 'bind');
    sinon.stub(Mousetrap, 'unbind');
  });
  afterEach(() => {
    Mousetrap.bind.restore();
    Mousetrap.unbind.restore();
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<BrowseTable {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should render SearchField and VirtualizedTableViewer', () => {
    const wrapper = shallow(<BrowseTable {...commonProps}/>);
    expect(wrapper).to.have.length(1);
    expect(wrapper.find('SearchField')).to.have.length(1);
    expect(wrapper.find('StatefulTableViewer')).to.have.length(1);
  });
  describe('#componentDidMount', () => {
    it('should call Mousetrap.bind', () => {
      const wrapper = shallow(<BrowseTable {...commonProps}/>);
      const instance = wrapper.instance();
      instance.componentDidMount();
      expect(Mousetrap.bind).to.been.called;
    });
  });
  describe('#componentWillUnmount', () => {
    it('should call Mousetrap.unbind', () => {
      const wrapper = shallow(<BrowseTable {...commonProps}/>);
      const instance = wrapper.instance();
      instance.componentWillUnmount();
      expect(Mousetrap.unbind).to.been.called;
    });
  });
  describe('#handleFilterChange', () => {
    it('should change state.filter', () => {
      const wrapper = shallow(<BrowseTable {...commonProps}/>);
      const instance = wrapper.instance();
      instance.handleFilterChange('filter');
      instance.handleFilterChange.flush();
      expect(wrapper.state('filter')).to.eql('filter');
    });
  });
  describe('#filteredTableData', () => {
    it('should filter tableData based on state.filter', () => {
      const wrapper = shallow(<BrowseTable {...commonProps}/>);
      const instance = wrapper.instance();
      wrapper.setState({
        filter: 'f'
      });
      const filteredTableData = instance.filteredTableData();
      expect(filteredTableData).to.have.size(1);
      expect(filteredTableData.get(0).data.name.value).to.eql('foo');
    });
  });
});
