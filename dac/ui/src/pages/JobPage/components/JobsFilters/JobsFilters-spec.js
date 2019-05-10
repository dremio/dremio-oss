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

import JobsFilters from './JobsFilters';
import * as IntervalTypes from './StartTimeSelect/IntervalTypes';


describe('JobsFilters', () => {

  let minimalProps;
  let commonProps;
  let context;
  beforeEach(() => {
    minimalProps = {
      onUpdateQueryState: sinon.spy(),
      loadItemsForFilter: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      queryState: Immutable.fromJS({
        filters: {qt: ['UI', 'EXTERNAL']}
      })
    };
    context = {
      loggedInUser: {admin: true}
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<JobsFilters {...minimalProps}/>, {context});
    expect(wrapper).to.have.length(1);
  });

  it('should render ContainsText, SelectMenu', () => {
    const wrapper = shallow(<JobsFilters {...commonProps}/>, {context});
    expect(wrapper.find('ContainsText')).to.have.length(1);
    expect(wrapper.find('Select')).to.have.length(1);
  });

  describe('handlers -', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<JobsFilters {...commonProps}/>, {context});
      instance = wrapper.instance();
    });

    describe('addInfoToFilter', () => {
      it('should update the queryState.filter for type', () => {
        wrapper.instance().addInfoToFilter('foo', 'bar');
        const result = commonProps.queryState.setIn(['filters', 'foo'], Immutable.List(['bar']));
        expect(commonProps.onUpdateQueryState.getCall(0).args[0].equals(result)).to.be.true;

        // doesn't add duplicate
        wrapper.instance().addInfoToFilter('foo', 'bar');
        expect(commonProps.onUpdateQueryState.getCall(1).args[0].equals(result)).to.be.true;
      });
    });

    describe('removeInfoFromFilter', () => {
      it('should not trigger update if nothing to remove', () => {
        wrapper.instance().removeInfoFromFilter('foo', 'bar');
        expect(commonProps.onUpdateQueryState).to.not.have.been.called;

        wrapper.instance().removeInfoFromFilter('qt', 'UI');

        const result = commonProps.queryState.setIn(['filters', 'qt'], Immutable.List(['EXTERNAL']));
        expect(commonProps.onUpdateQueryState.getCall(0).args[0].equals(result)).to.be.true;
      });
    });

    describe('toggleSortDirection', () => {
      it('should update queryState with order', () => {
        instance.toggleSortDirection();
        const result = commonProps.queryState.set('order', 'ASCENDING');
        expect(commonProps.onUpdateQueryState.getCall(0).args[0].toJS()).to.eql(result.toJS());

        wrapper.setProps({queryState: result});
        instance.toggleSortDirection();
        const result2 = commonProps.queryState.set('order', 'DESCENDING');
        expect(commonProps.onUpdateQueryState.getCall(1).args[0].toJS()).to.eql(result2.toJS());
      });
    });

    describe('changeSortItem', () => {
      it('should update queryState with sort', () => {
        instance.changeSortItem('id');
        const result = commonProps.queryState.set('sort', 'id').set('order', 'DESCENDING');
        expect(commonProps.onUpdateQueryState.getCall(0).args[0].toJS()).to.eql(result.toJS());
      });
    });

    describe('handleEnterText', () => {
      it('should update contains with text, and remove it if no text', () => {
        instance.handleEnterText('text');
        const result = commonProps.queryState.setIn(['filters', 'contains'], Immutable.List(['text']));
        expect(commonProps.onUpdateQueryState.getCall(0).args[0].toJS()).to.eql(result.toJS());

        instance.handleEnterText('');
        const result2 = commonProps.queryState.deleteIn(['filters', 'contains']);
        expect(commonProps.onUpdateQueryState.getCall(1).args[0].toJS()).to.eql(result2.toJS());
      });
    });

    describe('handleStartTimeChange', () => {
      it('should delete st if ALL_TIME_INTERVAL is used', () => {
        commonProps.queryState.setIn(['filters', 'st'], Immutable.List(['text']));
        instance.handleStartTimeChange(IntervalTypes.ALL_TIME_INTERVAL, []);
        expect(commonProps.queryState.get('filters').has('st')).to.eql(false);
      });
    });
  });

  describe('#getAllFilters', () => {
    it('should return false for User Filter if user is non-admin', () => {
      const wrapper =  shallow(<JobsFilters {...commonProps}/>, {context: { loggedInUser: {admin: true}}});
      const instance = wrapper.instance();
      expect(instance.getAllFilters().filter(filter => filter.value === 'usr')).to.have.length(1);

      wrapper.setContext({ loggedInUser: {admin: false} });
      expect(instance.getAllFilters().filter(filter => filter.value === 'usr')).to.have.length(0);
    });
  });
});
