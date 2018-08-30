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

import ViewStateWrapper from 'components/ViewStateWrapper';
import browserUtils from 'utils/browserUtils';
import StatefulTableViewer from './StatefulTableViewer';

describe('StatefulTableViewer', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      viewState: Immutable.Map(),
      tableData: Immutable.List(),
      columns: []
    };
    commonProps = {
      ...minimalProps,
      columns: [{
        key: 'col1',
        label: 'col1'
      },
      {
        key: 'col2',
        label: 'col2'
      }]
    };
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<StatefulTableViewer {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('render', function() {
    let getPlatformStub;
    beforeEach(() => {
      getPlatformStub = sinon.stub(browserUtils, 'getPlatform').returns({
        name: 'Chrome',
        version: 54
      });
    });
    afterEach(() => {
      getPlatformStub.restore();
    });
    it('should render VirtualizedTableViewer only if virtualized is true', () => {
      const wrapper = shallow(<StatefulTableViewer virtualized {...commonProps}/>);
      expect(wrapper.find('VirtualizedTableViewer')).to.have.length(1);
      wrapper.setProps({
        virtualized: false
      });

      expect(wrapper.find('VirtualizedTableViewer')).to.have.length(0);
      expect(wrapper.find('TableViewer')).to.have.length(1);
    });
    it('should pass correct viewState to ViewStateWrapper', function() {
      const viewState = Immutable.fromJS({ isInProgress: true });
      const props = {
        ...commonProps,
        viewState
      };
      const wrapper = shallow(<StatefulTableViewer {...props}/>, { context });
      expect(wrapper.find(ViewStateWrapper).prop('viewState')).to.be.eql(viewState);
    });

    it('should not render VirtualizedTableViewer when tableData is empty for IE 11', () => {
      getPlatformStub.returns({name: 'IE', version: 11});
      const wrapper = shallow(<StatefulTableViewer virtualized {...commonProps}/>);
      expect(wrapper.find('VirtualizedTableViewer')).to.have.length(0);
    });
  });

  describe('renderTableContent', () => {
    it('should render only table header when table is loading w/ ViewStateWrapper below', () => {
      const props = {
        ...commonProps,
        viewState: Immutable.fromJS({ isInProgress: true })
      };
      const wrapper = shallow(<StatefulTableViewer {...props}/>, { context });
      expect(wrapper.childAt(0).is('TableViewer')).to.be.true;
      expect(wrapper.childAt(1).is(ViewStateWrapper)).to.be.true;

      expect(wrapper.children()).to.have.length(2);

      expect(wrapper.find('TableViewer').prop('tableData')).to.be.empty;
    });
    it('should render table when table data is not empty', () => {
      const props = {
        ...commonProps,
        viewState: Immutable.fromJS({ isInProgress: false }),
        tableData: Immutable.List([{data: { col1: 'v1', col2: 'v2'}}])
      };
      const wrapper = shallow(<StatefulTableViewer {...props}/>, { context });
      expect(wrapper.find('TableViewer')).to.have.length(1);
    });
    it('should render table headers and No Data message when table data is empty', () => {
      const props = {
        ...commonProps,
        viewState: Immutable.fromJS({ isInProgress: false }),
        tableData: Immutable.fromJS([])
      };
      const wrapper = shallow(<StatefulTableViewer {...props}/>, { context });
      expect(wrapper.find('TableViewer')).to.have.length(1);
      expect(wrapper.find('div.empty-message')).to.have.length(1);
    });

    it('should render errors w/ ViewStateWrapper on top', () => {
      const props = {
        ...commonProps,
        viewState: Immutable.fromJS({ error: {message: 'foo'} }),
        tableData: Immutable.List([{data: { col1: 'v1', col2: 'v2'}}])
      };
      const wrapper = shallow(<StatefulTableViewer {...props}/>, { context });
      expect(wrapper.childAt(0).is(ViewStateWrapper)).to.be.true;
      expect(wrapper.childAt(1).is('TableViewer')).to.be.true;
      expect(wrapper.children()).to.have.length(2);
    });
  });
});
