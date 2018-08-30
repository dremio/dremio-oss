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
import { SelectedTextPopoverView as SelectedTextPopover } from './SelectedTextPopover';

describe('SelectedTextPopover', () => {
  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      location: {
        pathname: 'ds1',
        state: {}
      }
    };
    commonProps = {
      ...minimalProps,
      location: {
        ...minimalProps.location,
        query: {}
      }
    };
    wrapper = shallow(<SelectedTextPopover {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<SelectedTextPopover {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('renderItems', () => {
    beforeEach(() => {
      sinon.stub(instance, 'renderForItemsOfList');
      sinon.stub(instance, 'renderItem');
    });
    afterEach(() => {
      instance.renderForItemsOfList.restore();
      instance.renderItem.restore();
    });

    it('should call renderItem', () => {
      instance.renderItems();
      expect(instance.renderForItemsOfList.called).to.be.false;
      expect(instance.renderItem.called).to.be.true;
    });

    it('should call renderForItemsOfList if location.state has listOfItems', () => {
      const location = {
        ...commonProps.location,
        state: { listOfItems: ['Extract', 'Replace', 'Split', 'Keep Only', 'Exclude'] } // todo: why does this use UI strings?
      };
      wrapper.setProps({ location });
      expect(instance.renderForItemsOfList.calledOnce).to.be.true;
      expect(instance.renderItem.called).to.be.false;
    });
  });

  describe('getItemsForColumnType', () => {
    it('should return filtered items if type !== TEXT && type !== LIST && type !== MAP', () => {
      expect(instance.getItemsForColumnType('INTEGER')).to.eql(
        instance.items.filter(item => item.get('name') !== 'Extract…' && item.get('name') !== 'Split…')
      );
    });

    it('should return all items if type of TEXT or List or MAP', () => {
      expect(instance.getItemsForColumnType('TEXT')).to.eql(instance.items);
      expect(instance.getItemsForColumnType('MAP')).to.eql(instance.items);
      expect(instance.getItemsForColumnType('LIST')).to.eql(instance.items);
    });
    it('should return empty list if type of BINARY or MIXED', () => {
      expect(instance.getItemsForColumnType('BINARY')).to.eql(Immutable.List());
      expect(instance.getItemsForColumnType('MIXED')).to.eql(Immutable.List());
    });
  });

  describe('renderItem', () => {
    let menuItemLink;
    beforeEach(() => {
      menuItemLink = shallow(
        instance.renderItem(Immutable.fromJS({transform: 'extract', name: 'Extract'}))
      );
    });
    it('should add transformType=item.transform to href.state', () => {
      expect(menuItemLink.instance().props.href.state).to.eql({ transformType: 'extract' });
    });
    it('should add type=transform to href.query', () => {
      expect(menuItemLink.instance().props.href.query).to.eql({ type: 'transform' });
    });
    it('should append /details to href.pathname', () => {
      expect(menuItemLink.instance().props.href.pathname).to.eql('ds1/details');
    });
  });

  describe('renderForItemsOfList', () => {
    it('should call renderItem with extract item', () => {
      sinon.spy(instance, 'renderItem');
      const newState = { columnName: 'revenue', columnType: 'INTEGER' };
      instance.renderForItemsOfList(newState);
      expect(instance.items.get(0).get('name')).to.eql('Extract…');
      expect(instance.renderItem).to.be.calledWith(instance.items.get(0), newState);
    });
  });

});
