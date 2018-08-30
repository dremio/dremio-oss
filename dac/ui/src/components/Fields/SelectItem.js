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
import Radium from 'radium';
import pureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import classNames from 'classnames';
import { itemWrapper, selected as selectedCls, disabled as disabledCls } from './Select.less';

export const CONTENT_WIDTH = 'calc(100% - 25px)';

@Radium
@pureRender
export default class SelectItem extends Component {
  static propTypes = {
    label: PropTypes.node.isRequired,
    dataQA: PropTypes.string,
    style: PropTypes.object,
    value: PropTypes.any,
    disabled: PropTypes.bool,
    onTouchTap: PropTypes.func,
    selected: PropTypes.bool,
    className: PropTypes.string
  }

  handleTouchTap = (event) => {
    event.preventDefault();

    if (this.props.onTouchTap) {
      this.props.onTouchTap(event);
    }
  }

  render() {
    const { disabled, style, label, selected, dataQA, className } = this.props;
    return (
      <div
        onTouchTap={this.handleTouchTap}
        style={[style]}
        className={classNames({
          [itemWrapper]: true,
          [className]: !!className,
          [selectedCls]: selected,
          [disabledCls]: disabled
        })}
        data-qa={dataQA || label}>
        {label}
      </div>
    );
  }
}
