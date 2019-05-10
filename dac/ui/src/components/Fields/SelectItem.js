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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';

import { itemWrapper, selected as selectedCls, disabled as disabledCls } from './Select.less';

export default class SelectItem extends PureComponent {
  static propTypes = {
    label: PropTypes.node.isRequired,
    dataQa: PropTypes.string,
    style: PropTypes.object,
    value: PropTypes.any,
    disabled: PropTypes.bool,
    onClick: PropTypes.func,
    selected: PropTypes.bool,
    className: PropTypes.string
  }

  handleTouchTap = (event) => {
    event.preventDefault();

    if (this.props.onClick) {
      this.props.onClick(event, this.props.value);
    }
  }

  render() {
    const { disabled, style, label, selected, dataQa, className } = this.props;
    return (
      <div
        onClick={disabled ? null : this.handleTouchTap}
        style={style}
        className={classNames({
          [itemWrapper]: true,
          [className]: !!className,
          [selectedCls]: selected,
          [disabledCls]: disabled
        })}
        data-qa={dataQa || label}>
        {label}
      </div>
    );
  }
}
