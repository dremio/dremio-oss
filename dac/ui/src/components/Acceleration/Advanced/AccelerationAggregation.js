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
import { Component, PropTypes } from 'react';
import Immutable from 'immutable';
import { Toggle } from 'components/Fields';
import FontIcon from 'components/Icon/FontIcon';
import SimpleButton from 'components/Buttons/SimpleButton';
import { commonStyles } from '../commonStyles';
import AccelerationGridController from './AccelerationGridController';

export default class AccelerationAggregation extends Component {
  static propTypes = {
    acceleration: PropTypes.instanceOf(Immutable.Map),
    fields: PropTypes.object
  };

  static getFields() {
    return [
      'aggregationLayouts.enabled',
      'aggregationLayouts.layoutList[].id.id',
      'aggregationLayouts.layoutList[].name',
      'aggregationLayouts.layoutList[].details.partitionFieldList[].name',
      'aggregationLayouts.layoutList[].details.sortFieldList[].name',
      'aggregationLayouts.layoutList[].details.dimensionFieldList[].name',
      'aggregationLayouts.layoutList[].details.dimensionFieldList[].granularity',
      'aggregationLayouts.layoutList[].details.measureFieldList[].name',
      'aggregationLayouts.layoutList[].details.distributionFieldList[].name'
    ];
  }

  static validate() {
    return {};
  }

  addNewLayout = () => {
    const { aggregationLayouts } = this.props.fields;
    aggregationLayouts.layoutList.addField({
      name: la('New Reflection'),
      details: {
        dimensionFieldList: [],
        measureFieldList: [],
        partitionFieldList: [],
        sortFieldList: []
      }
    });
  }

  removeLayout = columnIndex => {
    const { aggregationLayouts } = this.props.fields;
    if (aggregationLayouts.layoutList.length > 1) {
      aggregationLayouts.layoutList.removeField(columnIndex);
    }
  }

  renderHeader = () => {
    const { enabled } = this.props.fields.aggregationLayouts;
    const toggleLabel = (
      <div style={commonStyles.toggleLabel}>
        <FontIcon type='Aggregate' theme={commonStyles.iconTheme}/>
        {la('Aggregation Reflections')}
      </div>
    );
    return (
      <div style={commonStyles.header}>
        <Toggle {...enabled} label={toggleLabel} style={commonStyles.toggle}/>
        <SimpleButton
          onClick={this.addNewLayout}
          buttonStyle='secondary'
          children={la('New Reflection')}
          type='button'/>
      </div>
    );
  }

  render() {
    const {acceleration, fields: {aggregationLayouts}} = this.props;
    return (
      <div>
        {this.renderHeader()}
        <AccelerationGridController
          acceleration={acceleration}
          layoutFields={aggregationLayouts.layoutList}
          activeTab='aggregation'
          removeLayout={this.removeLayout}/>
      </div>
    );
  }
}
