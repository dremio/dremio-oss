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
import ApiUtils from 'utils/apiUtils/apiUtils';
import { UpdateDataset } from './UpdateDataset';


describe('UpdateDataset', () => {
  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      query: {name: 'ds1'},
      isOpen: false,
      loadDependentDatasets: sinon.spy(),
      removeFileFormat: sinon.spy(),
      convertDatasetToFolder: sinon.spy(),
      renameSpaceDataset: sinon.stub().returns(Promise.resolve()),
      moveDataSet: sinon.stub().returns(Promise.resolve()),
      removeDataset: sinon.stub().returns(Promise.resolve()),
      createDatasetFromExisting: sinon.stub().returns(Promise.resolve()),
      hide: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      item: Immutable.fromJS({
        resourcePath: '/dataset/"Prod-Sample".ds2',
        fullPathList: ['Prod-Sample', 'ds2'],
        entityType: 'dataset'
      })
    };
    wrapper = shallow(<UpdateDataset {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    const wrapperMin = shallow(<UpdateDataset {...minimalProps}/>);
    expect(wrapperMin).to.have.length(1);
  });

  describe('#receiveProps', () => {
    it('should call loadDependentDatasets with full path', () => {
      instance.receiveProps(commonProps, {});
      expect(commonProps.loadDependentDatasets).to.have.been.calledWith(commonProps.item.get('fullPathList'));
    });
  });

  describe('#moveDataset', () => {
    it('should call moveDataSet with pathFrom', () => {
      instance.moveDataset({ datasetName: 'ds3', selectedEntity: 'Prod-Sample' });
      expect(commonProps.moveDataSet).to.have.been.calledWith(
        commonProps.item.get('fullPathList'),
        ['Prod-Sample', 'ds3']
      );
    });
  });

  describe('#copyDataset', () => {
    it('should call copyDataset with pathFrom', () => {
      instance.copyDataset({ datasetName: 'ds3', selectedEntity: 'Prod-Sample2' });
      expect(commonProps.createDatasetFromExisting)
            .to.have.been.calledWith(commonProps.item.get('fullPathList'), ['Prod-Sample2', 'ds3'], { name: 'ds3' });
    });

    it('should call copyDataset w/o selected entity', () => {
      instance.copyDataset({ datasetName: 'ds3' });
      expect(commonProps.createDatasetFromExisting)
        .to.have.been.calledWith(commonProps.item.get('fullPathList'), ['Prod-Sample', 'ds3'], { name: 'ds3' });
    });
  });

  describe('#renameDataset', () => {
    it('should call renameSpaceDataset with pathFrom', () => {
      instance.renameDataset({ datasetName: 'ds12' });
      expect(commonProps.renameSpaceDataset).to.have.been.calledWith(commonProps.item, 'ds12');
    });
  });

  describe('#removeFormat', () => {
    it('should call convertDatasetToFolder for non file entity type', () => {
      instance.removeFormat();
      expect(commonProps.convertDatasetToFolder).to.have.been.calledWith(
        commonProps.item,
        'toggleFolderPhysicalDataset'
      );
    });
    it('should call removeFileFormat for a file', () => {
      const tempProps = {
        ...minimalProps,
        item: Immutable.fromJS({entityType: 'file'})
      };
      instance = shallow(<UpdateDataset {...tempProps}/>).instance();
      instance.removeFormat();
      expect(tempProps.removeFileFormat).to.have.been.calledWith(tempProps.item);
    });
  });

  describe('#submit', () => {
    beforeEach(() => {
      sinon.spy(ApiUtils, 'attachFormSubmitHandlers');
    });
    afterEach(() => {
      ApiUtils.attachFormSubmitHandlers.restore();
    });

    it('should call props.renameSpaceDataset when keyAction = renameDataset', () => {
      instance.submit('renameDataset', {datasetName: 'ds1'});
      expect(commonProps.renameSpaceDataset).to.be.calledWith(commonProps.item, 'ds1');
      expect(ApiUtils.attachFormSubmitHandlers).to.be.calledOnce;
    });

    it('should call props.moveDataSet when keyAction = moveDataset', () => {
      instance.submit('moveDataset', { datasetName: 'ds3', selectedEntity: 'Prod-Sample' });
      expect(commonProps.moveDataSet).to.be.calledWith(commonProps.item.get('fullPathList'), ['Prod-Sample', 'ds3']);
      expect(ApiUtils.attachFormSubmitHandlers).to.be.calledOnce;
    });

    it('should call props.createDatasetFromExisting when keyAction = copyDataset', () => {
      instance.submit('copyDataset',  { datasetName: 'ds3', selectedEntity: 'Prod-Sample' });
      expect(commonProps.createDatasetFromExisting).to.be.calledWith(
        commonProps.item.get('fullPathList'),
        ['Prod-Sample', 'ds3']
      );
      expect(ApiUtils.attachFormSubmitHandlers).to.be.calledOnce;
    });
  });
});
