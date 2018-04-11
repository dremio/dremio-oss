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
import fileUtils from 'utils/fileUtils/fileUtils';
import FileField from './FileField';

describe('FileField', () => {
  let minimalProps;
  let commonProps;
  let instance;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      style: {},
      value: '',
      onChange: sinon.spy()
    };
    instance = shallow(<FileField {...commonProps}/>).instance();
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<FileField {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<FileField {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render Dropzone with common props', () => {
    const wrapper = shallow(<FileField {...commonProps}/>);
    expect(wrapper.find('Dropzone')).to.have.length(1);
  });

  describe('#getFileName', () => {
    it('should return empty when value not defined', () => {
      expect(instance.getFileName(null)).to.be.empty;
    });

    it('should return empty when name not defined', () => {
      expect(instance.getFileName({})).to.be.empty;
    });

    it('should return "name1" when name defined', () => {
      expect(instance.getFileName({name: 'name1'})).to.be.equal('name1');
    });
  });

  describe('#getFileSize', () => {

    beforeEach(() => {
      sinon.stub(fileUtils, 'convertFileSize').returns('1MB');
    });

    afterEach(() => {
      fileUtils.convertFileSize.restore();
    });

    it('should return empty when value not defined', () => {
      expect(instance.getFileSize(null)).to.be.empty;
    });

    it('should return empty when size not defined', () => {
      expect(instance.getFileSize({})).to.be.empty;
    });

    it('should return "1MB" when size defined', () => {
      expect(instance.getFileSize({size: '1'})).to.be.equal('1MB');
    });
  });

  describe('#startProgress', () => {

    it('should set current timestamp', () => {
      instance.startProgress();
      expect(instance.state.loadProgressTime).to.be.not.equal(0);
    });
  });

  describe('#endProgress', () => {

    it('should reset state properties to zero', () => {
      instance.endProgress();
      expect(instance.state).to.be.eql({
        _radiumStyleState: {},
        loadProgressTime: 0,
        total: 0,
        loaded: 0,
        loadSpeed: 0
      });
    });
  });

  describe('#updateProgress', () => {

    beforeEach(() => {
      sinon.stub(instance, 'calculateLoadSpeed').returns('1MB/s');
    });

    afterEach(() => {
      instance.calculateLoadSpeed.restore();
    });

    it('should update state properties', () => {
      instance.updateProgress({loaded: '1', total: '2'});
      expect(instance.state.total).to.be.equal(2);
      expect(instance.state.loaded).to.be.equal(1);
      expect(instance.state.loadSpeed).to.be.equal('1MB/s');
    });
  });

  describe('#calculateLoadSpeed', () => {

    it('should return "1.0" when loaded 1MB in 1 second', () => {
      instance.setState({
        loaded: 0,
        loadProgressTime: 0
      });
      expect(instance.calculateLoadSpeed(1024 * 1024, 1000)).to.be.equal('1.0');
    });
  });
});
