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
import { sortBy } from 'lodash/collection';
import { injectIntl } from 'react-intl';
import PropTypes from 'prop-types';

import SelectConnectionButton from 'components/SelectConnectionButton';
import { sourceTypesIncludeS3 } from 'utils/sourceUtils';
import { isExternalSourceType, isDatalakeTableSourceType, isDataPlaneSourceType } from '@app/constants/sourceTypes.js';

import 'pages/HomePage/components/modals/AddSourceModal/SelectSourceType.less';

@injectIntl
export default class SelectSourceType extends Component {
  static propTypes = {
    onSelectSource: PropTypes.func,
    sourceTypes: PropTypes.array,
    intl: PropTypes.object.isRequired,
    isExternalSource: PropTypes.bool,
    isDataPlaneSource: PropTypes.bool
  };

  getEnabledSourceTypes(allTypes) {
    return sortBy(allTypes.filter(type => !type.disabled), ['label']);
  }

  getDisabledSourceTypes(allTypes) {
    return sortBy(allTypes.filter(type => type.disabled), ['enterprise', 'label']);
  }

  renderSourceTypes(connections) {
    const { intl } = this.props;
    return connections.map((item) => {
      let pillText = '';
      let isCommunity = false;
      if (item.disabled) {
        pillText = intl.formatMessage({ id: 'Source.CommingSoonTag' });
      } else if (item.tags && item.tags.includes('beta')) {
        pillText = intl.formatMessage({ id: 'Source.BetaTag' });
      } else if (item.tags && item.tags.includes('community')) {
        pillText = intl.formatMessage({ id: 'Source.CommunityTag' });
        isCommunity = true;
      }

      return <SelectConnectionButton
        label={item.label}
        pillText={pillText}
        isCommunity={isCommunity}
        disabled={item.disabled}
        iconType={`sources/${item.sourceType}`}
        icon={item.icon}
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

  renderExternalSources() {
    const { sourceTypes } = this.props;
    const externalSources = sourceTypes.filter(source => isExternalSourceType(source.sourceType));
    return (
      <div className='SelectSourceType'>
        <div className='main'>
          <div className='source-type-section'>
            { this.renderSourceTypes(this.getEnabledSourceTypes(externalSources)) }
          </div>
          <div className='source-type-section'>
            { this.renderSourceTypes(this.getDisabledSourceTypes(externalSources)) }
          </div>
        </div>
      </div>
    );
  }

  renderDataPlanSources() {
    const { sourceTypes } = this.props;
    const dataPlaneSources = sourceTypes.filter(source => isDataPlaneSourceType(source.sourceType));
    return (
      <div className='SelectSourceType'>
        <div className='main'>
          <div className='source-type-section'>
            { this.renderSourceTypes(this.getEnabledSourceTypes(dataPlaneSources)) }
          </div>
          <div className='source-type-section'>
            { this.renderSourceTypes(this.getDisabledSourceTypes(dataPlaneSources)) }
          </div>
        </div>
      </div>
    );
  }

  renderDataLakeSources() {
    const { sourceTypes, intl } = this.props;
    const fileStoreSources = sourceTypes.filter(source => (
      !isExternalSourceType(source.sourceType) && !isDatalakeTableSourceType(source.sourceType) &&
      !isDataPlaneSourceType(source.sourceType)
    ));
    const tableStoreSources = sourceTypes.filter(source => isDatalakeTableSourceType(source.sourceType));
    return (
      <div className='SelectSourceType'>
        <div className='main'>
          <div className='source-type-section'>
            <div className='source-type-header'>
              {intl.formatMessage({ id: 'Source.TableStores' })}
            </div>
            { this.renderSourceTypes(this.getEnabledSourceTypes(tableStoreSources)) }
            { this.renderSourceTypes(this.getDisabledSourceTypes(tableStoreSources)) }
          </div>
          <div className='source-type-section'>
            <div className='source-type-header'>
              {intl.formatMessage({ id: 'Source.FileStores' })}
            </div>
            { this.renderSourceTypes(this.getEnabledSourceTypes(fileStoreSources)) }
            { sourceTypesIncludeS3(sourceTypes) && this.renderSampleSource() }
            { this.renderSourceTypes(this.getDisabledSourceTypes(fileStoreSources)) }
          </div>
        </div>
      </div>
    );
  }

  render() {
    const { isExternalSource, isDataPlaneSource} = this.props;
    /*eslint no-nested-ternary: "off"*/
    return isExternalSource ? this.renderExternalSources() :
      isDataPlaneSource ? this.renderDataPlanSources() : this.renderDataLakeSources();
  }
}
