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
import { Component } from 'react';
import PropTypes from 'prop-types';
import ImmutablePropTypes from 'react-immutable-proptypes';
import classNames from 'classnames';
import keyCodes from '@app/constants/Keys.json';
import {
  container,
  input as inputCls,
  tagElement as tagElementCls,
  tagWrapper,
  cursorPlaceholder
} from './Tags.less';
import { Tag } from './Tag';



// component consist of 2 main sections:
// 1) Tags section. The section where tags are displayed
// 2) Input section. The section where a text input is located. User types a new tag here.
// I will references on this section through the code.
export class TagsView extends Component {
  static propTypes = {
    placeholder: PropTypes.string,
    tags: ImmutablePropTypes.listOf(PropTypes.string).isRequired,
    className: PropTypes.string,

    //if this handler is not provided, then input for new tags will not be displayed
    onAddTag: PropTypes.func, // (tagName: string) => void
    // If this handler is not provided, then x button for tags will not be displayed
    onRemoveTag: PropTypes.func, // (tagName: string) => Promise
    onTagClick: PropTypes.func, // (tagName: string) => void
    onChange: PropTypes.func // () => void is called, when tag is added or removed
  };

  state = {
    value: '', // value of input
    selectedTagIndex: -1 // a zero-base index of a selected tag. -1 means that cursor in an input.
  }

  handleTextChange = (e) => {
    const value = e.target.value;
    this.setState({
      value
    });
  }

  addTag = (newTag) => {
    const {
      onAddTag
    } = this.props;

    if (!newTag.trim()) return;

    onAddTag(newTag);
    this.setState({
      value: '' // string was transformed to a tags
    });
    this.onChange();
  }

  showInputField = () => !!this.props.onAddTag;

  onKeyDown = (e) => {
    const {
      tags
    } = this.props;

    const {
      selectedTagIndex,
      value
    } = this.state;
    const areTagsFocused = this.areTagsFocused();
    let preventDefault = false; // indicates if we should cancel a default behaviour for current key

    switch (e.keyCode) {
    case keyCodes.ENTER:
      this.addTag(value);
      break;
    case keyCodes.TAB:
      if (value) {
        preventDefault = true;
        this.addTag(value);
      }
      break;
    case keyCodes.HOME:
    case keyCodes.UP:
      this.moveCursorToTags(0);
      break;
    case keyCodes.END:
    case keyCodes.DOWN:
      if (this.showInputField()) {
        this.setInputPosition(this.input.value.length);
      } else {
        this.moveCursorToTags(tags.size - 1);
      }
      break;
    case keyCodes.LEFT:
      if (areTagsFocused) {
        this.moveCursorToTags(-1, { isRelative: true });
      } else if (this.showInputField() && this.input.selectionStart === 0) {
        this.moveCursorToTags(tags.size - 1);
      }
      break;
    case keyCodes.RIGHT:
      if (areTagsFocused) {
        this.moveCursorToTags(1, { isRelative: true });
      }
      //else do nothing. Default input behaviour is ok
      break;
    case keyCodes.BACKSPACE:
      if (areTagsFocused) {
        this.removeTag(selectedTagIndex);
      // cursor in the begining of the input and nothing is selected
      } else if (this.showInputField() && this.input.selectionStart === 0 && this.input.selectionEnd === 0) {
        this.moveCursorToTags(tags.size - 1);
      }
      break;
    case keyCodes.DELETE:
      if (areTagsFocused) {
        this.removeTag(selectedTagIndex, true);
      }
      break;
    default:
      break;
    }

    if (preventDefault) {
      e.preventDefault();
    }
  }

  areTagsFocused() {
    return this.state.selectedTagIndex >= 0;
  }

  // this method manages a cursor position inside tags section
  // positionOrOffset - a cursor position that should be set or offset relative to current cursor position
  moveCursorToTags(positionOrOffset, {
      isRelative = false
    } = {}) {
    let focusInput = false;
    this.setState((
      /* state */ {
        selectedTagIndex
      },
      /* props */ {
        tags
      }) => {
      const cnt = tags.size;
      const cursorPosition = isRelative ? selectedTagIndex + positionOrOffset : positionOrOffset; // a cursor position to be set

      focusInput = this.showInputField() && (cnt === 0 || cnt <= cursorPosition); // indidcates if an input should be selected after cursor position move. This may happen if position > number of tags. Or if there are no tags at all.

      const newState = {
        selectedTagIndex: focusInput ? -1 : Math.min(Math.max(cursorPosition, 0), cnt - 1)
      };

      return newState;
    }, () => {
      if (focusInput) {
        // has to do it via timeout, as if do this synchroniously, then cursor is set into second position. Default input behaviour cause this
        setTimeout(() => {
          this.setInputPosition(0);
        }, 0);
      } else {
        if (this.showInputField()) {
          this.input.blur();
        }
        this.tagsContainer.focus();
      }
    });
  }

  removeTag = async (index, selectNext) => {
    const {
      onRemoveTag,
      tags
    } = this.props;
    await onRemoveTag(tags.get(index));
    this.moveCursorToTags(selectNext ? index : (index - 1));

    this.onChange();
  }

  onChange = () => {
    const {
      onChange
    } = this.props;

    if (onChange) {
      onChange();
    }
  }

  focus() {
    if (!this.showInputField()) return;
    this.input.focus();
  }

  setInputPosition(position) {
    if (!this.showInputField()) return;
    this.input.focus();
    this.input.selectionStart = this.input.selectionEnd = position;
  }

  focusInput = () => {
    this.setState({
      selectedTagIndex: -1
    });
  }

  onInputRef = (element) => {
    this.input = element;
  }

  onTagsRef = (element) => {
    this.tagsContainer = element;
  }

  onTagClick = (tag) => {
    const { onTagClick } = this.props;

    if (onTagClick) {
      onTagClick(tag);
    }
  };

  render() {
    const {
      tags,
      placeholder,
      className
    } = this.props;
    const {
      value,
      selectedTagIndex
    } = this.state;

    const tagElements = [];

    tags.forEach((t, i) => {
      // I have to wrap this element to a span, as for multiline case tags has to go on new line with cursor placeholder.
      // This will keep left padding consistent.
      tagElements.push(<span key={`w_${i}`} className={tagWrapper}>
        <div key={`cursor_${i}`} className={cursorPlaceholder}
          ></div>
        <Tag key={`tag_${i}`}
          className={tagElementCls}
          text={t}
          daqa={`tag-pill-${t}`}
          deleteHandler={this.props.onRemoveTag ? () => this.removeTag(i) : null}
          onClick={() => this.onTagClick(t)}
          isSelected={i === selectedTagIndex}
          />
      </span>);
    });

    return (
      <div className={classNames(container, className)} tabIndex={0} onKeyDown={this.onKeyDown} ref={this.onTagsRef} data-qa='tagsContainer'>
        {tagElements}
        {
          this.showInputField() && <input type='text'
          tabIndex={0}
          placeholder={placeholder || la('Add tag')}
          ref={this.onInputRef}
          className={inputCls}
          value={value}
          onChange={this.handleTextChange}
          maxLength={128} // restriction according to spec
          onFocus={this.focusInput} />
        }
      </div>
    );
  }
}
