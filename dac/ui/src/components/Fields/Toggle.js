/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import FormControlLabel from '@material-ui/core/FormControlLabel';
import Switch from '@material-ui/core/Switch';
import { ToggleWithMixin } from '@inject/components/Fields/ToggleMixin.js';

export class Toggle extends Component {
  static propTypes = {
    onChange: PropTypes.func,
    value: PropTypes.bool,
    label: PropTypes.node,
    style: PropTypes.object,
    size: PropTypes.any,
    className: PropTypes.any
  }

  static defaultProps = {
    labelPosition: 'right'
  }

  render() {
    const { onChange, value, label, style, size, className } = this.props;
    const conditionalRenderingButtonStyling = this.checkToRenderToggle();
    return (
      <FormControlLabel
        control={ conditionalRenderingButtonStyling ? (
          <Switch
            color='primary'
            onChange={onChange}
            checked={value}
            className='field'
            size={size}
          />
        ) : ( <div style={{marginLeft: 15}}></div> )
        // DX-34369: do we need this marginLeft?
        }
        label={label}
        className={className ? className : null}
        style={{ marginRight: 0, ...style}}
      />
    );
  }
}

export default ToggleWithMixin(Toggle);
