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
import ColumnActionMenu from './ColumnActionMenu';

describe('ColumnActionMenu', () => {
  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      onRename: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      columnType: 'TEXT',
      columnName: 'user',
      columnsCount: 1,
      makeTransform: sinon.spy(),
      openDetailsWizard: sinon.spy(),
      hideDropdown: sinon.spy()
    };
    wrapper = shallow(<ColumnActionMenu {...commonProps}/>);
    instance = wrapper.instance();
  });
  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<ColumnActionMenu {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should render ExploreMenu, SortGroup, MainActionGroup, SqlGroup, ReplaceGroup, OtherGroup', () => {
    expect(wrapper.find('ExploreMenu')).to.have.length(1);
    expect(wrapper.find('SortGroup')).to.have.length(1);
    expect(wrapper.find('MainActionGroup')).to.have.length(1);
    expect(wrapper.find('SqlGroup')).to.have.length(1);
    expect(wrapper.find('ReplaceGroup')).to.have.length(1);
    expect(wrapper.find('OtherGroup')).to.have.length(1);
  });

  describe('#makeTransform', () => {
    it('should call props.makeTransform for one step items', () => {
      instance.makeTransform('ASC');
      expect(commonProps.onRename).to.not.be.called;
      expect(commonProps.makeTransform).to.be.called;
    });

    it('should call props.onRename if actionType == RENAME', () => {
      instance.makeTransform('RENAME');
      expect(commonProps.onRename).to.be.called;
      expect(commonProps.makeTransform).to.not.be.called;
    });

    it('should otherwise call openDetailsWizard', () => {
      instance.makeTransform('FOO');
      expect(commonProps.onRename).to.not.be.called;
      expect(commonProps.makeTransform).to.not.be.called;
      expect(commonProps.openDetailsWizard).to.be.called;
    });
  });

  describe('#isAvailable', () => {
    it('should return true only if for one of the menuItems columnType is available', () => {
      const menuItems = [
        { props: { availableTypes: ['TEXT'] }},
        { props: { availableTypes: ['FLOAT'] }}
      ];
      expect(instance.isAvailable(menuItems, 'TEXT')).to.be.true;
      expect(instance.isAvailable(menuItems, 'INTEGER')).to.be.false;
    });
  });
});
