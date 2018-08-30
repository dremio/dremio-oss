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
import PropTypes from 'prop-types';
import SampleDataMessage from 'pages/ExplorePage/components/SampleDataMessage';
import classNames from 'classnames';
import { base, warning, buttons } from './WizardFooter.less';

@Radium
export default class WizardFooter extends Component {
  static propTypes = {
    children: PropTypes.node,
    style: PropTypes.object
  }

  renderPreviewWarning() {
    return (
      <SampleDataMessage />
    );
  }

  render() {
    return (
      <div className={classNames(['wizard-footer', base])} style={[this.props.style]}>
        <div className={buttons}>
          {this.props.children}
        </div>
        <div className={warning}>
          {this.renderPreviewWarning()}
        </div>
      </div>
    );
  }
}
