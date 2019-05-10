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

import {DatasetMenu} from './DatasetMenu';

describe('DatasetMenu', () => {

  let minimalProps;
  let commonProps;

  const context = {context: {location: {bar: 2, state: {foo: 1}}}};
  beforeEach(() => {
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
      entityType: 'fake',

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

  it('#getGraphUrl()', () => {
    const instance = shallow(<DatasetMenu {...commonProps} />, context).instance();
    expect(instance.getMenuItemUrl('graph')).to.equal('/vgf/graph?qwe');
  });

  describe('#getRenameLocation', () => {
    it('should return location to UpdateDataset modal with mode=rename, item=entity, and name=datasetName', () => {
      const instance = shallow(<DatasetMenu {...commonProps} />, context).instance();
      const result = instance.getRenameLocation();
      expect(result.state.modal).to.equal('UpdateDataset');
      expect(result.state.item).to.equal(commonProps.entity);
      expect(result.state.query.mode).to.equal('rename');
      expect(result.state.query.name).to.equal(commonProps.entity.get('datasetName'));
    });

    it('should maintain existing location.state', () => {
      const instance = shallow(<DatasetMenu {...commonProps} />, context).instance();
      const result = instance.getRenameLocation();
      expect(result.state.foo).to.equal(1);
    });
  });

  describe('#getMoveLocation', () => {
    it('should return location to UpdateDataset modal with mode=move, item=entity, and name=datasetName', () => {
      const instance = shallow(<DatasetMenu {...commonProps} />, context).instance();
      const result = instance.getMoveLocation();
      expect(result.state.modal).to.equal('UpdateDataset');
      expect(result.state.item).to.equal(commonProps.entity);
      expect(result.state.query.mode).to.equal('move');
      expect(result.state.query.name).to.equal(commonProps.entity.get('datasetName'));
    });

    it('should maintain existing location.state', () => {
      const instance = shallow(<DatasetMenu {...commonProps} />, context).instance();
      const result = instance.getRenameLocation();
      expect(result.state.foo).to.equal(1);
    });
  });

  describe('#getRemoveLocation', () => {
    it('should return location to UpdateDataset modal with mode=remove, item=entity, and name=datasetName', () => {
      const instance = shallow(<DatasetMenu {...commonProps} />, context).instance();
      const result = instance.getRemoveLocation();
      expect(result.state.modal).to.equal('UpdateDataset');
      expect(result.state.item).to.equal(commonProps.entity);
      expect(result.state.query.mode).to.equal('remove');
      expect(result.state.query.name).to.equal(commonProps.entity.get('datasetName'));
    });

  });


  describe('#getSettingsLocation()', () => {
    it('should return location to DatasetSettingsModal with props.entityType and fullPath', () => {
      const instance = shallow(<DatasetMenu {...commonProps} />, context).instance();
      const result = instance.getSettingsLocation();
      expect(result.state.modal).to.equal('DatasetSettingsModal');
      expect(result.state.entityType).to.equal(commonProps.entityType);
    });

    it('should get entityId for virtual dataset (versionedResource)', () => {
      const wrapper = shallow(<DatasetMenu {...commonProps} />, context);
      expect(wrapper.instance().getSettingsLocation().state.entityId).to.eql(
        commonProps.entity.get('versionedResourcePath')
      );
    });

    it('should get entityId for file, folder, PDS (id)', () => {
      const file = commonProps.entity.set('id', 'someId').remove('versionedResourcePath');
      const wrapper = shallow(<DatasetMenu {...commonProps} entity={file}/>, context);
      expect(wrapper.instance().getSettingsLocation().state.entityId).to.eql('someId');
    });
  });

  describe('#handleRemoveFile()', () => {
    it('should show confirmation dialog before remove', () => {
      const instance = shallow(<DatasetMenu {...commonProps} />, context).instance();
      instance.handleRemoveFile();
      expect(commonProps.showConfirmationDialog).to.be.called;
      expect(commonProps.closeMenu).to.be.called;
      expect(commonProps.removeFile).to.not.be.called;
    });

    it('should call remove file when confirmed', () => {
      const props = {
        ...commonProps,
        showConfirmationDialog: (opts) => opts.confirm()
      };
      const instance = shallow(<DatasetMenu {...props} />, context).instance();
      instance.handleRemoveFile();
      expect(commonProps.removeFile).to.be.called;
      expect(props.closeMenu).to.be.called;
    });
  });

});
