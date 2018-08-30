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

export default class AccelerationAggregation extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    reflections: PropTypes.instanceOf(Immutable.Map).isRequired,
    fields: PropTypes.object
  };

  static getFields() {
    return [
      'aggregationReflections[].id',
      'aggregationReflections[].tag',
      'aggregationReflections[].type',
      'aggregationReflections[].name',
      'aggregationReflections[].enabled',
      'aggregationReflections[].partitionDistributionStrategy',
      'aggregationReflections[].partitionFields[].name',
      'aggregationReflections[].sortFields[].name',
      'aggregationReflections[].dimensionFields[].name',
      'aggregationReflections[].dimensionFields[].granularity',
      'aggregationReflections[].measureFields[].name',
      'aggregationReflections[].measureFields[].measureTypeList',
      'aggregationReflections[].distributionFields[].name',
      'aggregationReflections[].shouldDelete'
    ];
  }

  static validate() {
    return {};
  }

  addNewLayout = () => {
    const { aggregationReflections } = this.props.fields;

    const reflection = createReflectionFormValues({
      type: 'AGGREGATION'
    }, aggregationReflections.map(e => e.name.value));

    aggregationReflections.addField(reflection);
  };

  renderHeader = () => {
    return (
      <div style={commonStyles.header}>
        <h3 style={commonStyles.toggleLabel}>
          <FontIcon type='Aggregate' theme={commonStyles.iconTheme}/>
          {la('Aggregation Reflections')}
        </h3>
        <SimpleButton
          onClick={this.addNewLayout}
          buttonStyle='secondary'
          children={la('New Reflection')}
          type='button'/>
      </div>
    );
  };

  render() {
    const {dataset, reflections, fields: {aggregationReflections}} = this.props;
    return (
      <div style={styles.base}>
        {this.renderHeader()}
        <AccelerationGridController
          dataset={dataset}
          reflections={reflections}
          layoutFields={aggregationReflections}
          activeTab='aggregation'
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
