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

import Button from 'components/Buttons/Button';
import * as ButtonTypes from 'components/Buttons/ButtonTypes';
import WizardFooter from './WizardFooter';

@Radium
@pureRender
export default class StepWizard extends Component {
  static propTypes = {
    changeFormType: PropTypes.func.isRequired,
    onCancelClick: PropTypes.func.isRequired,
    onNextClick: PropTypes.func,
    style: PropTypes.object,
    hasActiveDataset: PropTypes.bool
  }

  render() {
    return (
      <WizardFooter style={this.props.style}>
        <Button
          disable={!this.props.hasActiveDataset}
          onMouseDown={this.props.changeFormType.bind(this, 'apply')}
          onClick={this.props.onNextClick}
          type={ButtonTypes.NEXT}
          disableSubmit
          key='details-wizard-next'
          text='Next'/>
        <Button
          style={{marginLeft: 5}}
          type={ButtonTypes.CANCEL}
          disableSubmit
          onClick={this.props.onCancelClick}
          key='details-wizard-cancel'/>
      </WizardFooter>
    );
  }
}
