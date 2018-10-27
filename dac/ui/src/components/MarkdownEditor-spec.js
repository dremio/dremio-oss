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
import { MarkdownEditorView } from './MarkdownEditor';

describe('MarkdownEditor', () => {
  it('MarkdownEditor could be in fullScreenMode only, when readMode is disabled', () => {
    const wrapper = shallow(<MarkdownEditorView readMode={false} />);

    wrapper.setState({
      fullScreenMode: true
    });

    wrapper.setProps({ readMode: true });

    expect(wrapper.state().fullScreenMode).to.eql(false); //state should be reset
  });

  describe('onReadModeHasScrollChanged', () => {
    const mockInstance = editorElement => {
      const wrapper = shallow(editorElement);

      wrapper.instance().editor = {}; // mock the editor

      return wrapper;
    };

    const simulateHasScrollChanged = (editorWrapper, hasScrollValue) => {
      const instance = editorWrapper.instance();
      sinon.stub(instance, 'hasScrollInReadMode');
      instance.hasScrollInReadMode.returns(hasScrollValue);

      instance._updateHasScrollImpl();

      instance.hasScrollInReadMode.restore();
    };

    //todo may be we should mock a debounce timeout somehow to not slow down the tests
    it('onReadModeHasScrollChanged is called only when hasScrollValue is changed', () => {
      const changeHandler = sinon.stub();
      const wrapper = mockInstance(<MarkdownEditorView
        readMode
        onReadModeHasScrollChanged={changeHandler}
        />);

      const testChange = (currentValue) => {
        simulateHasScrollChanged(wrapper, currentValue);
        expect(changeHandler).have.been.calledWith(currentValue); // change handler should be called with a currentValue.
      };

      testChange(true);
      testChange(false);
    });

    it('Editor should works without onReadModeHasScrollChanged in read mode', () => {
      // if this test fails, then somebody changed _updateHasScrollImpl method so onReadModeHasScrollChanged is called, when it is not provided.
      // Please fix _updateHasScrollImpl
      const wrapper = mockInstance(<MarkdownEditorView
        readMode
        />);

      simulateHasScrollChanged(wrapper, true); // there should not be any errors
    });
  });

  // other part is hard to test as it requires mounting of the react-simple-mde, which fails in test environment
});
