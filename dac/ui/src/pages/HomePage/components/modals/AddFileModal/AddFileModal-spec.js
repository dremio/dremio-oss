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

import { AddFileModal, PREVIEW_VIEW_ID } from './AddFileModal';

describe('AddFileModal', () => {

  let minimalProps;
  let commonProps;
  let context;
  beforeEach(() => {
    minimalProps = {
      loadFilePreview: sinon.stub(),
      uploadFinish: sinon.stub().resolves({}),
      uploadCancel: sinon.stub().resolves({}),
      destroy: sinon.stub(),
      resetViewState: sinon.stub()
    };
    commonProps = {
      isOpen: true,
      fileName: 'file.json',
      file: Immutable.Map({}),
      resetFileFormatPreview: sinon.stub().resolves({}),
      uploadFileToPath: sinon.stub().resolves({}),
      hide: sinon.spy(),
      ...minimalProps
    };
    context = {
      username: 'username'
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AddFileModal {...minimalProps} />, {context});
    expect(wrapper).to.have.length(1);
  });

  describe('#componentDidMount', () => {
    it('should call resetViewState', () => {
      const wrapper = shallow(<AddFileModal {...commonProps} />, {context});
      const instance = wrapper.instance();
      instance.componentWillMount();
      expect(commonProps.resetViewState).to.have.been.calledWith(PREVIEW_VIEW_ID);
    });
  });

  describe('#componentWillReceiveProps', () => {
    it('should call cancelUpload if filename changes', () => {
      const wrapper = shallow(<AddFileModal {...commonProps} />, {context});
      wrapper.setProps({fileName: 'file2.json'});
      expect(commonProps.uploadCancel).to.have.been.called;
    });
  });

  describe('#onSubmitFile', () => {
    it('should successfully resolve promise', () => {
      const wrapper = shallow(<AddFileModal {...commonProps} />, {context});
      const instance = wrapper.instance();

      const promise = instance.onSubmitFile({file: '', name: '', extension: ''});
      return expect(promise).to.be.fulfilled;
    });

    it('should reject the promise if an error occurs', () => {
      const props = {
        ...commonProps, uploadFileToPath: sinon.stub().rejects({
          error: {
            response: {
              errorMessage: 'foo'
            }
          }
        })
      };
      const wrapper = shallow(<AddFileModal {...props} />, {context});
      const instance = wrapper.instance();

      const promise = instance.onSubmitFile({file: '', name: '', extension: ''});
      return expect(promise).to.be.rejected;
    });
  });

  describe('#onSubmitFormat', () => {
    it('should successfully call hide', () => {
      const wrapper = shallow(<AddFileModal {...commonProps} />, {context});
      const instance = wrapper.instance();

      const promise = instance.onSubmitFormat({});
      return expect(promise).to.be.fulfilled;
    });
  });
});
