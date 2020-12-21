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
import React, { Component } from 'react';
import Radium from 'radium';

import PropTypes from 'prop-types';

import { Tooltip } from 'components/Tooltip';
import HoverHelp from 'components/HoverHelp';

import forms from 'uiTheme/radium/forms';
import classNames from 'classnames';

@Radium
export default class FieldWithError extends Component {

  static defaultProps = {
    errorPlacement: 'right'
  };

  static propTypes = {
    label: PropTypes.string,
    touched: PropTypes.bool,
    error: PropTypes.string,
    errorPlacement: PropTypes.string,
    style: PropTypes.object,
    labelStyle: PropTypes.object,
    labelClass: PropTypes.string,
    children: PropTypes.node.isRequired,
    name: PropTypes.string,
    hoverHelpText: PropTypes.string,
    className: PropTypes.string,
    labelTooltip: PropTypes.string
  };

  constructor(props) {
    super(props);

    this.start = {
      overLabel: false
    };
  }

  onMouseEnterLabel = () => {
    this.setState({overLabel: true});
  }

  onMouseLeaveLabel = () => {
    this.setState({overLabel: false});
  }

  render() {
    const {style, children, touched, error, errorPlacement, name, className} = this.props;

    const showError = Boolean(touched && error);

    return (
      <div
        className={classNames({
          'field-with-error field': true,
          [className]: !!className
        })}
        data-qa={name} style={{...style, position:'relative'}}>
        {this.renderLabel()}
        {React.cloneElement(React.Children.only(children), {ref: 'target'})}
        <Tooltip
          container={this}
          target={() => showError ? this.refs.target : null}
          placement={errorPlacement}
          type='error'
          dataQa='error'
        >
          {error}
        </Tooltip>
      </div>
    );
  }

  renderLabel() {
    const {label, hoverHelpText, labelClass, labelTooltip} = this.props;
    const {overLabel} = this.state;

    const hoverHelp = hoverHelpText ? <HoverHelp content={hoverHelpText} /> : null;

    // todo: <label> is not correctly associated with the input here (i.e. it is broken and doing nothing)
    // todo: hoverHelp should not be inside the <label>
    return (label &&
    <div>
      <label
        className={classNames({
          [labelClass]: !!labelClass
        })}
        style={[forms.label, styles.label, this.props.labelStyle]}
      >
        <div
          onMouseEnter={this.onMouseEnterLabel}
          onMouseLeave={this.onMouseLeaveLabel}
          ref='labelTarget'
        >
          {label}
        </div>
        {hoverHelp}
      </label>
      <Tooltip
        container={this}
        target={() => overLabel && labelTooltip ? this.refs.labelTarget : null}
        placement={'top-start'}
        type='status'
        dataQa='status'
        style={{zIndex:40000}}
      >
        {labelTooltip}
      </Tooltip>
    </div>
    )
    ;
  }
}

const styles = {
  label: {
    display: 'flex',
    alignItems: 'center',
    color: '#7F8B95',
    fontWeight: 400,
    height: '32px',
    marginBottom: '0px'
  }
};
