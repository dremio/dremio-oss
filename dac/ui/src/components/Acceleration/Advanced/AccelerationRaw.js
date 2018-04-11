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
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import FontIcon from 'components/Icon/FontIcon';
import SimpleButton from 'components/Buttons/SimpleButton';
import {createReflectionFormValues} from 'utils/accelerationUtils';

import { commonStyles } from '../commonStyles';
import AccelerationGridController from './AccelerationGridController';

export default class AccelerationRaw extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    reflections: PropTypes.instanceOf(Immutable.Map).isRequired,
    fields: PropTypes.object
  };

  static getFields() {
    return [
      'rawReflections[].id',
      'rawReflections[].tag',
      'rawReflections[].type',
      'rawReflections[].name',
      'rawReflections[].enabled',
      'rawReflections[].partitionDistributionStrategy',
      'rawReflections[].partitionFields[].name',
      'rawReflections[].sortFields[].name',
      'rawReflections[].displayFields[].name',
      'rawReflections[].distributionFields[].name',
      'rawReflections[].shouldDelete'
    ];
  }

  static validate() {
    return {};
  }

  addNewLayout = () => {
    const { rawReflections } = this.props.fields;

    const reflection = createReflectionFormValues({
      type: 'RAW'
    }, rawReflections.map(e => e.name.value));

    rawReflections.addField(reflection);
  }

  renderHeader = () => {
    return (
      <div style={commonStyles.header}>
        <h3 style={commonStyles.toggleLabel}>
          <FontIcon type='RawMode' theme={commonStyles.iconTheme}/>
          {la('Raw Reflections')}
        </h3>
        <SimpleButton
          onClick={this.addNewLayout}
          buttonStyle='secondary'
          children={la('New Reflection')}
          type='button'/>
      </div>
    );
  }

  render() {
    const {dataset, reflections, fields: {rawReflections}} = this.props;
    return (
      <div style={styles.base}>
        {this.renderHeader()}
        <AccelerationGridController
          dataset={dataset}
          reflections={reflections}
          layoutFields={rawReflections}
          activeTab='raw'
          />
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 1
  }
};
