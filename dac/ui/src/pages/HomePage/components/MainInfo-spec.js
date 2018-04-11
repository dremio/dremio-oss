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
import DatasetMenu from 'components/Menus/HomePage/DatasetMenu';
import FolderMenu from 'components/Menus/HomePage/FolderMenu';
import { UnformattedEntityMenu } from 'components/Menus/HomePage/UnformattedEntityMenu';
import MainInfo from './MainInfo';

describe('MainInfo', () => {

  let minimalProps;
  let commonProps;
  let context;
  beforeEach(() => {
    context = {
      location: {
        pathname: ''
      }
    };
    minimalProps = Immutable.fromJS({});
    commonProps = Immutable.fromJS([{
      entity: Immutable.Map({
        contents: []
      }),
      entityType: 'type',
      viewState: Immutable.Map(),
      updateRightTreeVisibility: sinon.spy(),
      rightTreeVisible: true,
      isInProgress: false
    }]);
  });


  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<MainInfo {...minimalProps} />, { context });
    expect(wrapper).to.have.length(1);
  });

  it('should render commpn props', () => {
    const wrapper = shallow(<MainInfo {...commonProps}/>, { context });
    expect(wrapper).to.have.length(1);
  });

  describe('#getRow', function() {

    it('should return descendants from extendedConfig when entityType is Folder', function() {
      const instance = shallow(<MainInfo {...commonProps}/>, { context }).instance();
      sinon.stub(instance, 'getButtonsDataCell').returns('action');
      const item = Immutable.fromJS({
        descendants: 0,
        entityType: 'folder',
        links: {
          query: 'queryLink',
          jobs: 'jobsLink'
        },
        jobCount: 1,
        extendedConfig: {
          descendants: 2,
          datasetCount: 0
        }
      });
      expect(instance.getRow(item).data.descendants.node()).to.be.equal(2);
    });
  });

  describe('#getFolderActions', () => {

    it('should show setting button for folder-as-dataset', () => {
      const instance = shallow(<MainInfo {...commonProps}/>, { context }).instance();
      sinon.stub(instance, 'getSettingsBtnByType');
      const folder = Immutable.fromJS({
        fileSystemFolder: true,
        queryable: true
      });
      instance.getFolderActions(folder);

      expect(instance.getSettingsBtnByType).to.have.been.calledWith(<DatasetMenu entity={folder} entityType='folder'/>);
    });

    it('should show query button for folder-as-dataset', () => {
      const instance = shallow(<MainInfo {...commonProps}/>, { context }).instance();
      sinon.stub(instance, 'getSettingsBtnByType');
      const folder = Immutable.fromJS({
        fileSystemFolder: true,
        queryable: true
      });
      instance.getFolderActions(folder);

      expect(instance.getSettingsBtnByType).to.have.been.calledWith(<DatasetMenu entity={folder} entityType='folder'/>);
    });

    it('should show button for convert folder to folder-as-dataset', () => {
      const instance = shallow(<MainInfo {...commonProps}/>, { context }).instance();
      sinon.stub(instance, 'getSettingsBtnByType');
      const folder = Immutable.fromJS({
        fileSystemFolder: true,
        queryable: false,
        entityType: 'folder',
        id: 1
      });
      instance.getFolderActions(folder);

      expect(instance.getSettingsBtnByType).to.have.been.calledWith(
        <UnformattedEntityMenu entity={folder}/>
      );
    });

    it('should show settings button for folder', () => {
      const instance = shallow(<MainInfo {...commonProps}/>, { context }).instance();
      sinon.stub(instance, 'getSettingsBtnByType');
      const folder = Immutable.fromJS({
        fileSystemFolder: false,
        queryable: false
      });
      instance.getFolderActions(folder);

      expect(instance.getSettingsBtnByType).to.have.been.calledWith(<FolderMenu folder={folder}/>);
    });
  });
});
