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
import { sortBy } from 'lodash/collection';
import { injectIntl } from 'react-intl';
import PropTypes from 'prop-types';

import SelectConnectionButton from 'components/SelectConnectionButton';
import { FLEX_START_WRAP } from 'uiTheme/radium/flexStyle';

import 'pages/HomePage/components/modals/AddSourceModal/SelectSourceType.less';

@injectIntl
export default class SelectSourceType extends Component {
  static propTypes = {
    onSelectSource: PropTypes.func,
    sourceTypes: PropTypes.array,
    intl: PropTypes.object.isRequired
  };

  getEnabledSourceTypes(allTypes) {
    return sortBy(allTypes.filter(type => !type.disabled), ['label']);
  }

  getDisabledSourceTypes(allTypes) {
    return sortBy(allTypes.filter(type => type.disabled), ['enterprise', 'label']);
  }

  renderSourceTypes(connections) {
    return connections.map((item) => {
      let pillText = '';
      if (item.disabled) {
        pillText = la('coming soon');
      } else if (item.tags && item.tags.includes('beta')) {
        pillText = la('beta');
      }

      return <SelectConnectionButton
        label={item.label}
        pillText={pillText}
        disabled={item.disabled}
        iconType={`sources/${item.sourceType}`}
        key={item.sourceType}
        onClick={!item.disabled ? this.props.onSelectSource.bind(this, item) : undefined}/>;
    });
  }

  renderSampleSource() {
    return <SelectConnectionButton
      label={this.props.intl.formatMessage({ id: 'Source.SampleSource' })}
      iconType={'sources/SampleSource'}
      onClick={this.props.onSelectSource.bind(this, { sourceType: 'SampleSource'})}/>;
  }

  render() {
    return (
      <div className='SelectSourceType'>
        <div className='main'>
          <div className='source-type-section' style={FLEX_START_WRAP}>
            { this.renderSourceTypes(this.getEnabledSourceTypes(this.props.sourceTypes)) }
            { this.renderSampleSource() }
          </div>
          <div className='source-type-section' style={FLEX_START_WRAP}>
            { this.renderSourceTypes(this.getDisabledSourceTypes(this.props.sourceTypes)) }
          </div>
        </div>
      </div>
    );
  }
}
