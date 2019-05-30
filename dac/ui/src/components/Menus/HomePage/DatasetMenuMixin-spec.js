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

import { findMenuItemLinkByText, findMenuItemByText } from 'testUtil';
// test via DatasetMenu
import {DatasetMenu as DatasetMenuBase} from 'components/Menus/HomePage/DatasetMenu';
import DatasetMenuMixin from './DatasetMenuMixin';

// create clean copy for the class so that we don't mutate the normal one.
// this assumes that the DatasetMenuMixin implementations can replace the each other
// (since DatasetMenuBase will already have it applied)
@DatasetMenuMixin
class DatasetMenu extends DatasetMenuBase {}

describe('DatasetMenuMixin', () => {

  let minimalProps;
  let commonProps;

  const context = {context: {location: {bar: 2, state: {foo: 1}}}};
  beforeEach(() => { // todo: DRY
    minimalProps = {
      entity: Immutable.fromJS({ // todo: we need "stock" entity factories for testing
        versionedResourcePath: 'someVersionedResourcePath',
        fullPath: 'zig/zag',
        fullPathList: ['zig', 'zag'],
        datasetName: 'bob',
        links: {
          edit: '/foo?bar',
          self: '/sdc?aws',
          query: '/vgf?qwe'
        }
      }),
      entityType: 'dataset',

      closeMenu: sinon.stub(),
      removeDataset: sinon.stub(),
      removeFile: sinon.stub(),
      removeFileFormat: sinon.stub(),
      convertDatasetToFolder: sinon.stub(),
      showConfirmationDialog: sinon.spy()
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DatasetMenu {...minimalProps} />, context);
    expect(wrapper).to.have.length(1);
  });

  describe('render menu items', () => {
    let wrapper;
    beforeEach(() => {
      wrapper = shallow(<DatasetMenu {...commonProps}/>, context);
    });

    it('should render "Copy Path" when entity is file/folder/home_file', () => {
      wrapper.setProps({
        entityType: 'file'
      });
      expect(findMenuItemByText(wrapper, 'Copy Path')).to.have.length(1);

      wrapper.setProps({
        entityType: 'folder'
      });
      expect(findMenuItemByText(wrapper, 'Copy Path')).to.have.length(1);

      wrapper.setProps({
        entityType: 'file',
        entity: commonProps.entity.merge({
          isHomeFile: true
        })
      });
      expect(findMenuItemByText(wrapper, 'Copy Path')).to.have.length(1);
    });

    it('should render "Remove Format" only when entity is file/folder and not home file', () => {
      expect(findMenuItemLinkByText(wrapper, 'Remove Format')).to.have.length(0);

      wrapper.setProps({
        entityType: 'file'
      });
      expect(findMenuItemLinkByText(wrapper, 'Remove Format')).to.have.length(1);

      wrapper.setProps({
        entityType: 'folder'
      });
      expect(findMenuItemLinkByText(wrapper, 'Remove Format')).to.have.length(1);

      wrapper.setProps({
        entityType: 'file',
        entity: commonProps.entity.merge({
          isHomeFile: true
        })
      });
      expect(findMenuItemLinkByText(wrapper, 'Remove Format')).to.have.length(0);
    });

    it('should render "Move"/"Rename"/"Remove" when can', () => {
      // just using Move as a canary
      expect(findMenuItemLinkByText(wrapper, 'Move')).to.have.length(1);

      wrapper.setProps({
        entityType: 'folder'
      });
      expect(findMenuItemLinkByText(wrapper, 'Move')).to.have.length(0);

      wrapper.setProps({
        entityType: 'file'
      });
      expect(findMenuItemLinkByText(wrapper, 'Move')).to.have.length(0);

      wrapper.setProps({
        entityType: 'physicalDataset'
      });
      expect(findMenuItemLinkByText(wrapper, 'Move')).to.have.length(0);
    });

    it('should render "Edit" when entity not file/folder/physicalDataset', () => {
      expect(findMenuItemLinkByText(wrapper, 'Edit')).to.have.length(1);

      wrapper.setProps({
        entityType: 'folder'
      });
      expect(findMenuItemLinkByText(wrapper, 'Edit')).to.have.length(0);

      wrapper.setProps({
        entityType: 'file'
      });
      expect(findMenuItemLinkByText(wrapper, 'Edit')).to.have.length(0);

      wrapper.setProps({
        entityType: 'physicalDataset'
      });
      expect(findMenuItemLinkByText(wrapper, 'Edit')).to.have.length(0);

    });

    // feature has a bug see DX-7054
    it.skip('should render "Browse Contents" when entity type is folder', () => {
      expect(findMenuItemLinkByText(wrapper, 'Browse Contents')).to.have.length(0);

      wrapper.setProps({
        entityType: 'folder'
      });
      expect(findMenuItemLinkByText(wrapper, 'Browse Contents')).to.have.length(1);
    });
  });

});
