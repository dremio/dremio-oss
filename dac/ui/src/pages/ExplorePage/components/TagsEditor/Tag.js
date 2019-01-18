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
import classNames from 'classnames';
import Art from '@app/components/Art';
import {
  tag,
  selected,
  deletable,
  deleteButton,
  clickable,
  textWrapper,
  text as textClass
} from './Tag.less';

export class Tag extends Component {

  static propTypes = {
    text: PropTypes.string,
    readonly: PropTypes.bool,
    className: PropTypes.string,
    isSelected: PropTypes.bool,
    daqa: PropTypes.string,
    // true - use text as title,
    // false - do not show title
    // string - use provided string as a title
    title: PropTypes.oneOfType([PropTypes.string, PropTypes.bool]),
    //handlers
    onClick: PropTypes.func,
    deleteHandler: PropTypes.func,
    onRef: PropTypes.func
  };

  static defaultProps = {
    title: true
  };

  deleteClick = (e) => {
    e.stopPropagation();

    this.props.deleteHandler();
  };

  render() {
    const {
      text,
      readonly,
      deleteHandler,
      onClick,
      className,
      isSelected,
      daqa,
      title,
      onRef
    } = this.props;
    const isDeletable = !readonly && !!deleteHandler;

    // 'font-icon' and 'XBig' are global icon style classes
    const props = {
      className: classNames(tag, isDeletable && deletable, isSelected && selected,
        onClick && clickable, // show pointer if onClick handler is provided
        className),
      onClick,
      'data-qa': daqa
    };


    // set title if title property is true or a string
    if (title) {
      props.title = title === true ? text : title;
    }

    return (
      <div {...props} ref={onRef}>
        <span className={textWrapper}>
          <span className={textClass}>
            {text}
          </span>
        </span>
        {
          isDeletable &&
          <Art src='XBig.svg' alt='delete' title  className={classNames(deleteButton)} onClick={this.deleteClick}/>
        }
      </div>
    );
  }
}
