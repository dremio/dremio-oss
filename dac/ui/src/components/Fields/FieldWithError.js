/*
 * Copyright (C) 2017 Dremio Corporation
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
import ReactDOM from 'react-dom';
import { Overlay } from 'react-overlays';
import Radium from 'radium';

import PropTypes from 'prop-types';

import Tooltip from 'components/Tooltip';
import HoverHelp from 'components/HoverHelp';

import forms from 'uiTheme/radium/forms';

@Radium
export default class FieldWithError extends Component {

  static defaultProps = {
    errorPlacement: 'left'
  };

  static propTypes = {
    label: PropTypes.string,
    touched: PropTypes.bool,
    error: PropTypes.string,
    errorPlacement: PropTypes.string,
    style: PropTypes.object,
    labelStyle: PropTypes.object,
    children: PropTypes.node.isRequired,
    name: PropTypes.string,
    hoverHelpText: PropTypes.string
  };

  render() {
    const {style, children, touched, error, errorPlacement, name} = this.props;

    const showError = Boolean(touched && error);

    return (
      <div className='field-with-error' data-qa={name} style={{...style, position:'relative'}}>
        {this.renderLabel()}
        {React.cloneElement(React.Children.only(children), {ref: 'target'})}
        <Overlay
          show={showError}
          container={this}
          placement={errorPlacement}
          target={() => ReactDOM.findDOMNode(this.refs.target)}>
          <Tooltip type='error' placement='left' content={error} arrowOffsetLeft={0} arrowOffsetTop={0}/>
        </Overlay>
      </div>
    );
  }

  renderLabel() {
    const {label, hoverHelpText} = this.props;

    const hoverHelp = hoverHelpText ? <HoverHelp content={hoverHelpText} tooltipInnerStyle={styles.hoverTip} /> : null;

    // todo: <label> is not correctly associated with the input here (i.e. it is broken and doing nothing)
    // todo: hoverHelp should not be inside the <label>
    return label && <label style={[forms.label, styles.label, this.props.labelStyle]}>{label}{hoverHelp}</label>;
  }
}

const styles = {
  hoverTip: {
    textAlign: 'left',
    width: 300,
    whiteSpace: 'pre-line'
  },
  label: {
    display: 'flex',
    alignItems: 'center'
  }
};
