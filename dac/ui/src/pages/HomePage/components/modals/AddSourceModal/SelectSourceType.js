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
import { Component } from 'react';
import PropTypes from 'prop-types';
import { sortBy } from 'lodash/collection';
import { FormattedMessage, injectIntl } from 'react-intl';
import SelectConnectionButton from 'components/SelectConnectionButton';

import { FLEX_START_WRAP } from 'uiTheme/radium/flexStyle';

import { sourceProperties } from 'dyn-load/constants/sourceTypes';

import './SelectSourceType.less';

const enabledSourceTypes = sortBy(
  sourceProperties.filter((sourceProps) => !sourceProps.disabled),
  ['label']
);

const disabledSourceTypes = sortBy(
  sourceProperties.filter((sourceProps) => sourceProps.disabled),
  ['enterprise', 'label']
);

@injectIntl
export default class SelectSourceType extends Component {
  static propTypes = {
    onSelectSource: PropTypes.func.isRequired,
    onAddSampleSource: PropTypes.func.isRequired,
    intl: PropTypes.object.isRequired
  };
  renderSourceTypes(connections) {
    return connections.map((item) => {
      let pillText = '';
      if (item.disabled) {
        pillText = la('coming soon');
      } else if (item.beta) {
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

  render() {
    return (
      <div className='SelectSourceType'>
        <div className='main'>
          <h3 className='normalWeight'>
            <FormattedMessage id = 'Source.DataSourceTypes'/>
          </h3>
          <div className='source-type-section' style={FLEX_START_WRAP}>
            { this.renderSourceTypes(enabledSourceTypes) }
            <SelectConnectionButton
              label={this.props.intl.formatMessage({ id: 'Source.SampleSource' })}
              iconType={'sources/SampleSource'}
              onClick={this.props.onAddSampleSource}/>
          </div>
          <div className='source-type-section' style={FLEX_START_WRAP}>
            { this.renderSourceTypes(disabledSourceTypes) }
          </div>
        </div>
      </div>
    );
  }
}
