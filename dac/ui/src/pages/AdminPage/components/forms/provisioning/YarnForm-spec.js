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
import Immutable from 'immutable';
import {shallow} from 'enzyme';
import {minimalFormProps} from 'testUtil';
import * as PROVISION_DISTRIBUTIONS from 'constants/provisioningPage/provisionDistributions';
import {YarnForm} from './YarnForm';

describe('YarnForm', () => {
  let minimalProps;
  let fields;
  let commonProps;
  beforeEach(() => {
    const fieldNames = ['clusterType', 'resourceManagerHost', 'namenodeHost', 'queue',
      'memoryMB', 'virtualCoreCount', 'id'];
    const formProps = minimalFormProps(fieldNames);
    fields = {
      ...formProps.fields,
      dynamicConfig: {
        containerCount: { onChange: sinon.spy() }
      },
      propertyList: [],
      spillDirectories: [{ onChange: sinon.spy()}]
    };
    minimalProps = {
      ...formProps,
      onFormSubmit: sinon.spy(),
      provision: Immutable.Map(),
      fields
    };
    commonProps = {
      ...minimalProps,
      values: {
        distroType: 'MAPR',
        spillDirectories: ['maprfs:///var/mapr/local/${NM_HOST}/mapred/spill'],
        namenodeHost: 'maprfs:///'
      }
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<YarnForm {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('#componentWillReceiveProps', () => {
    let wrapper;
    beforeEach(() => {
      wrapper = shallow(<YarnForm {...commonProps}/>);
    });

    it('should update spillDirectories field according to selected distribution', () => {
      wrapper.setProps({ values: {...commonProps.values, distroType: 'OTHER'}});
      expect(commonProps.fields.spillDirectories[0].onChange).to.be.calledWith('file:///var/log/dremio');
      wrapper.setProps({ values: {...commonProps.values, distroType: 'MAPR',
        spillDirectories: ['file:///var/log/dremio']}});
      expect(commonProps.fields.spillDirectories[0].onChange)
        .to.be.calledWith('maprfs:///var/mapr/local/${NM_HOST}/mapred/spill');
    });

    it('should not update spillDirectories on distroType change if user changed its value', () => {
      wrapper.setProps({ values: {...commonProps.values, spillDirectories: ['/custom/dir']}});
      wrapper.setProps({ values: {...commonProps.values, distroType: 'OTHER', spillDirectories: ['/custom/dir']}});
      expect(commonProps.fields.spillDirectories[0].onChange).to.be.not.called;
    });

    it('should update namenodeHost field according to selected distribution', () => {
      wrapper.setProps({ values: {...commonProps.values, distroType: 'OTHER'}});
      expect(commonProps.fields.namenodeHost.onChange).to.be.calledWith('');
      wrapper.setProps({ values: {...commonProps.values, distroType: 'MAPR', namenodeHost: ''}});
      expect(commonProps.fields.namenodeHost.onChange).to.be.calledWith('maprfs:///');
    });

    it('should not update namenodeHost field when user changed its value', () => {
      wrapper.setProps({ values: {...commonProps.values, namenodeHost: 'custom_host'}});
      wrapper.setProps({ values: {...commonProps.values, distroType: 'OTHER', namenodeHost: 'custom_host'}});
      expect(commonProps.fields.namenodeHost.onChange).to.be.not.called;
    });
  });

  describe('#getIsRestartRequired', () => {
    let wrapper, instance;
    beforeEach(() => {
      wrapper = shallow(<YarnForm {...minimalProps}/>);
      instance = wrapper.instance();
    });

    it('return false if not edit mode', () => {
      expect(instance.getIsRestartRequired()).to.be.false;
    });
    it('return false if currentState is not RUNNING', () => {
      wrapper.setProps({provision: Immutable.fromJS({currentState: 'STOPPED'})});
      expect(instance.getIsRestartRequired()).to.be.false;
    });
    it('return false if there is nothing dirty', () => {
      wrapper.setProps({provisionId: 'foo'});
      expect(instance.getIsRestartRequired()).to.be.false;
    });
    it('return false if only dynamicConfig children dirty', () => {
      wrapper.setProps({provisionId: 'foo'});
      fields.dynamicConfig.containerCount.dirty = true;
      expect(instance.getIsRestartRequired()).to.be.false;
    });
    it('return true if non-dynamicConfig children dirty', () => {
      wrapper.setProps({provision: Immutable.fromJS({id: 'foo', currentState: 'RUNNING'})});
      fields.dynamicConfig.containerCount.dirty = true;
      fields.queue.dirty = true;
      expect(instance.getIsRestartRequired()).to.be.true;

      fields.queue.dirty = false;
      fields.propertyList.push({name:{dirty:true}, value:{}});
      expect(instance.getIsRestartRequired()).to.be.true;

      fields.propertyList.length = 0;
      fields.propertyList.push({name:{}, value:{dirty:true}});
      expect(instance.getIsRestartRequired()).to.be.true;

      fields.propertyList.length = 0;
      fields.spillDirectories.length = 0;
      fields.spillDirectories.push({dirty: true});
      expect(instance.getIsRestartRequired()).to.be.true;
    });
  });

  describe('#mapToFormFields', () => {
    it('should generate appropriate form fields from provision entity', () => {
      const provision = Immutable.fromJS({
        memoryMB: 1200,
        clusterType: 'YARN',
        virtualCoreCount: 1,
        dynamicConfig: {
          containerCount: 2
        },
        subPropertyList: [
          {
            key: 'yarn.resourcemanager.hostname',
            value: 'localhost',
            type: 'type'
          },
          {
            key: 'fs.defaultFS',
            value: 'hdfs://localhost',
            type: 'type'
          },
          {
            key: 'paths.spilling',
            value: JSON.stringify(['/path1', '/path2']),
            type: 'type'
          }
        ]
      });
      expect(YarnForm.mapToFormFields(provision)).to.be.eql({
        clusterType: 'YARN',
        dynamicConfig: {
          containerCount: 2
        },
        memoryMB: '1.17',
        namenodeHost: 'hdfs://localhost',
        propertyList: [],
        spillDirectories: ['/path1', '/path2'],
        resourceManagerHost: 'localhost',
        subPropertyList: [
          {
            key: 'yarn.resourcemanager.hostname',
            value: 'localhost',
            type: 'type'
          },
          {
            key: 'fs.defaultFS',
            value: 'hdfs://localhost',
            type: 'type'
          },
          {
            key: 'paths.spilling',
            value: JSON.stringify(['/path1', '/path2']),
            type: 'type'
          }
        ],
        virtualCoreCount: 1
      });
    });
  });

  describe('#normalizeValues', () => {
    it('should return value to be submitted as entity', () => {
      const values = {
        clusterType: 'YARN',
        dynamicConfig: {
          containerCount: 2
        },
        memoryMB: '1.17',
        namenodeHost: 'hdfs://localhost',
        propertyList: [],
        resourceManagerHost: 'localhost',
        virtualCoreCount: 1
      };
      expect(YarnForm.normalizeValues(values)).to.be.eql({
        clusterType: 'YARN',
        dynamicConfig: {
          containerCount: 2
        },
        memoryMB: 1198.08,
        subPropertyList: [
          {
            key: 'fs.defaultFS',
            value: 'hdfs://localhost'
          },
          {
            key: 'yarn.resourcemanager.hostname',
            value: 'localhost'
          }
        ],
        virtualCoreCount: 1
      });
    });
  });

  describe('#hostNameLabel', () => {
    it('should return "CLDB" when selected distroType is MAPR', () => {
      expect(YarnForm.hostNameLabel({distroType: 'MAPR'})).to.be.eql('CLDB');
    });

    it('should return "NameNode" by default and all other distro types', () => {
      const { MAPR, ...other} = PROVISION_DISTRIBUTIONS;
      Object.keys(other).forEach(distroType => {
        expect(YarnForm.hostNameLabel({distroType})).to.be.eql('NameNode');
      });
    });
  });

  describe('#hostNamePrefix', () => {
    it('should return "maprfs:///" when selected distroType is MAPR', () => {
      expect(YarnForm.hostNamePrefix('MAPR')).to.be.eql('maprfs:///');
    });

    it('should return empty string by default and all other distro types', () => {
      const { MAPR, ...other} = PROVISION_DISTRIBUTIONS;
      Object.keys(other).forEach(distroType => {
        expect(YarnForm.hostNamePrefix(distroType)).to.be.eql('');
      });
    });
  });

  describe('#distributionDirectory', () => {
    it('should return appropriate directory when selected distroType is MAPR', () => {
      expect(YarnForm.distributionDirectory('MAPR')).to.be.eql('maprfs:///var/mapr/local/${NM_HOST}/mapred/spill');
    });

    it('should return "file:///var/log/dremio" by default and all other distro types', () => {
      const { MAPR, ...other} = PROVISION_DISTRIBUTIONS;
      Object.keys(other).forEach(distroType => {
        expect(YarnForm.distributionDirectory(distroType)).to.be.eql('file:///var/log/dremio');
      });
    });
  });
});
