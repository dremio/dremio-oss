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
import { shallow, mount } from 'enzyme';
import { List } from 'immutable';
import keyCodes from '@app/constants/Keys.json';
import { TagsView } from './Tags';
import {
  deleteButton
} from './Tag.less';

const getTagName = index => `tag ${index + 1}`;
const generateTags = numberOfTags => List(Array(numberOfTags).fill().map((e, index) => getTagName(index)));
const simulateKeyDown = (wrapper, keyCode) => wrapper.simulate('keydown', {
  keyCode,
  preventDefault: () => {}
});
const getInput = wrapper => wrapper.find('input');
const setValueToInput = (wrapper, value) => {
  const input = getInput(wrapper);

  input.simulate('change', { target: { value } });
};
const getAllTags = wrapper => wrapper.find('Tag');
const clickTag = (wrapper, index) => {
  getAllTags(wrapper).at(index).simulate('click');
};
const setSelectedTag = (wrapper, index) => {
  wrapper.setState({
    selectedTagIndex: index
  });
};

describe('Tags', () => {
  let commonProps;
  const tagsCount = 5;

  beforeEach(() => {
    commonProps = {
      tags: generateTags(tagsCount),
      onAddTag: () => {},
      onRemoveTag: () => {}
    };
  });

  it('Right number of items is rendered', () => {

    const wrapper = shallow(<TagsView {...commonProps} />);

    expect(getAllTags(wrapper).length).to.eq(tagsCount);
  });

  const addTagTest = keyCodeName => {
    if (!keyCodes.hasOwnProperty(keyCodeName)) {
      throw new Error(`keyCodeName (${keyCodeName}) must be presented in keyCodes`);
    }

    it(`Add tag is called, when ${keyCodeName} is pressed. Tag selection is reset.`, () => {
      const addHandler = sinon.spy();
      const wrapper = shallow(<TagsView {...commonProps} onAddTag={addHandler} />);
      const instance = wrapper.instance();
      const tagName = 'test tag name with space';

      setValueToInput(wrapper, tagName);

      expect(instance.state.value).to.eq(tagName); // check that value is set to a state

      expect(addHandler).to.be.not.called;

      simulateKeyDown(wrapper, keyCodes[keyCodeName]);

      expect(addHandler).to.be.calledWith(tagName);
    });
  };

  ['ENTER', 'TAB'].forEach(addTagTest);

  it('Tab does not adds a tag if input is empty', () => {
    const addHandler = sinon.spy();
    const wrapper = shallow(<TagsView {...commonProps} onAddTag={addHandler} />);

    setValueToInput(wrapper, ''); // nothin is entered

    expect(addHandler).to.be.not.called;

    simulateKeyDown(wrapper, keyCodes.TAB);

    expect(addHandler).to.be.not.called;
  });

  it('onTagClick is called on tag click', () => {
    const clickHandler = sinon.spy();
    const wrapper = mount(<TagsView {...commonProps} onTagClick={clickHandler} />);
    const index = 2;
    clickTag(wrapper, index);

    expect(clickHandler).have.been.calledWith(getTagName(index));
  });

  describe('cursor move', () => { // here we have to use mount, as we need access to dom element to handle focus

    const homeTest = keyName => it(`Cursor is moved to a first tag, when ${keyName} is pressed.`, () => {
      const wrapper = mount(<TagsView {...commonProps} />);
      const instance = wrapper.instance();
      instance.moveCursorToTags(tagsCount - 1, true); // move a cursor to a tag

      simulateKeyDown(wrapper, keyCodes[keyName]);

      expect(instance.state.selectedTagIndex).to.eq(0);
    });

    // array values should respect keyCodes field names
    ['HOME', 'UP'].forEach(homeTest);

    const endTest = keyName => it(`Cursor is moved to a last character, when ${keyName} is pressed.`, () => {
      const wrapper = mount(<TagsView {...commonProps} />);
      const instance = wrapper.instance();
      const str = 'a tag name';
      setValueToInput(wrapper, str);

      simulateKeyDown(wrapper, keyCodes[keyName]);

      expect(instance.state.selectedTagIndex).to.eq(-1); // cursor should be located in input, so selectedTagIndex should be reset to -1

      const inputWrapper = getInput(wrapper);

      expect(inputWrapper.instance().selectionStart).to.eq(str.length); // check that a cursor in END position
    });

    // array values should respect keyCodes field names
    ['END', 'DOWN'].forEach(endTest);

    it('left arrow is pressed', () => {
      const wrapper = mount(<TagsView {...commonProps} />);
      const instance = wrapper.instance();
      const index = 3;

      setSelectedTag(wrapper, index); // set initial cursor position

      simulateKeyDown(wrapper, keyCodes.LEFT);

      expect(instance.state.selectedTagIndex).to.eq(index - 1);
    });

    it('cursor is moved to tags section, if left arrow is pressed and cursor in the begining of the input section', () => {
      const wrapper = mount(<TagsView {...commonProps} />);
      const instance = wrapper.instance();
      getInput(wrapper).simulate('focus'); // input is empty and focus in its begining
      expect(instance.state.selectedTagIndex).to.eq(-1); // means input is selected

      simulateKeyDown(getInput(wrapper), keyCodes.LEFT);
      expect(instance.state.selectedTagIndex).to.eq(commonProps.tags.size - 1); // means a cursor is moved to the tags section right before last tag;
    });

    it('right arrow is pressed', () => {
      const wrapper = mount(<TagsView {...commonProps} />);
      const instance = wrapper.instance();
      const index = 3;

      setSelectedTag(wrapper, index); // set initial cursor position

      simulateKeyDown(wrapper, keyCodes.RIGHT);

      expect(instance.state.selectedTagIndex).to.eq(index + 1);
    });
  });

  describe('tags deletion', () => {
    let wrapper;
    let removeHandler;

    beforeEach(() => {
      removeHandler = sinon.spy();
      wrapper = mount(<TagsView {...commonProps} onRemoveTag={removeHandler} />);
    });

    const deleteTest = isBACKSPACE => it(`tag is removed using ${isBACKSPACE ? 'BACKSPACE' : 'DELETE'} button`, () => {
      const position = 2;
      const keyCode = keyCodes[isBACKSPACE ? 'BACKSPACE' : 'DELETE'];
      setSelectedTag(wrapper, position); // move cursor to a position before 3rd tag (< tagsCount)

      simulateKeyDown(wrapper, keyCode);
      expect(removeHandler).have.been.calledWith(commonProps.tags.get(position)); // DELETE handler called for a next to the cursor tag
    });

    deleteTest(true);
    deleteTest(false);

    it('tag is removed on x click', () => {
      const index = 3;
      getAllTags(wrapper).at(index).find(`.${deleteButton}`).hostNodes().simulate('click');

      expect(removeHandler.calledWith(commonProps.tags.get(index))).to.eq(true); // DELETE handler called for a next to the cursor tag
    });
  });
});
