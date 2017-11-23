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
import Immutable from 'immutable';
import { Toggle } from 'components/Fields';
import FontIcon from 'components/Icon/FontIcon';
import SimpleButton from 'components/Buttons/SimpleButton';
import {getUniqueName} from 'utils/pathUtils';
import { commonStyles } from '../commonStyles';
import AccelerationGridController from './AccelerationGridController';

export default class AccelerationRaw extends Component {
  static propTypes = {
    acceleration: PropTypes.instanceOf(Immutable.Map),
    fields: PropTypes.object
  };

  static getFields() {
    return [
      'rawLayouts.layoutList[].id',
      'rawLayouts.layoutList[].name',
      'rawLayouts.layoutList[].details.partitionDistributionStrategy',
      'rawLayouts.layoutList[].details.partitionFieldList[].name',
      'rawLayouts.layoutList[].details.sortFieldList[].name',
      'rawLayouts.layoutList[].details.displayFieldList[].name',
      'rawLayouts.layoutList[].details.distributionFieldList[].name'
    ];
  }

  static validate() {
    return {};
  }


  addNewLayout = () => {
    const { rawLayouts } = this.props.fields;
    const name = getUniqueName(la('New Reflection'), proposedName => {
      return !rawLayouts.layoutList.some(layout => layout.name.value === proposedName);
    });

    rawLayouts.layoutList.addField({
      name,
      details: {
        displayFieldList: [],
        partitionFieldList: [],
        sortFieldList: [],
        partitionDistributionStrategy: 'CONSOLIDATED'
      }
    });
  }

  removeLayout = columnIndex => {
    const { rawLayouts } = this.props.fields;
    if (rawLayouts.layoutList.length > 1) {
      rawLayouts.layoutList.removeField(columnIndex);
    }
  }

  renderHeader = () => {
    const { enabled } = this.props.fields.rawLayouts;
    const toggleLabel = (
      <h3 style={commonStyles.toggleLabel}>
        <FontIcon type='RawMode' theme={commonStyles.iconTheme}/>
        {la('Raw Reflections')}
      </h3>
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
    const {acceleration, fields: {rawLayouts}} = this.props;
    return (
      <div style={styles.base}>
        {this.renderHeader()}
        <AccelerationGridController
          acceleration={acceleration}
          layoutFields={rawLayouts.layoutList}
          activeTab='raw'
          removeLayout={this.removeLayout}/>
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
