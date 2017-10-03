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
import pureRender from 'pure-render-decorator';
import Radium from 'radium';
import Immutable from 'immutable';
import FontIcon from 'components/Icon/FontIcon';
import { Toggle } from 'components/Fields';
import AggregateForm from 'components/Aggregate/AggregateForm';
import { splitFullPath } from 'utils/pathUtils';
import { commonStyles } from '../commonStyles';
import LayoutInfo from '../LayoutInfo';

@pureRender
@Radium
export default class AccelerationAggregate extends Component {
  static getFields() {
    return AggregateForm.getFields();
  }
  static validate(values) {
    return AggregateForm.validate(values);
  }
  static propTypes = {
    acceleration: PropTypes.instanceOf(Immutable.Map),
    fullPath: PropTypes.string,
    location: PropTypes.object,
    fields: PropTypes.object.isRequired,
    style: PropTypes.object,
    shouldHighlight: PropTypes.bool
  };

  static defaultProps = {
    acceleration: Immutable.Map(),
    fields: {}
  };

  mapToDataset(props) {
    return Immutable.fromJS({
      displayFullPath: props.fullPath && splitFullPath(props.fullPath)
    });
  }

  mapSchemaToColumns(acceleration) {
    const datasetSchema = acceleration.getIn(['context', 'datasetSchema', 'fieldList']) || Immutable.List();

    return datasetSchema.map((item, index) => {
      return Immutable.fromJS({
        type: item.get('type'),
        name: item.get('name'),
        index
      });
    });
  }

  render() {
    const { location, fields, acceleration, style } = this.props;
    const dataset = this.mapToDataset(this.props);
    const { enabled } = fields.aggregationLayouts || {};

    const firstLayout = acceleration.getIn('aggregationLayouts.layoutList.0'.split('.'));

    const columns = this.mapSchemaToColumns(acceleration);
    const toggleLabel = (
      <div style={commonStyles.toggleLabel}>
        <FontIcon type='Aggregate' theme={commonStyles.iconTheme}/>
        {la('Aggregation Reflections (Automatic)')}
      </div>
    );
    return (
      <div style={[style, styles.wrap]}>
        <div style={{
          ...commonStyles.header,
          ...(this.props.shouldHighlight ? commonStyles.highlight : {})
        }} data-qa='aggregation-queries-toggle'>
          <Toggle {...enabled} label={toggleLabel} style={commonStyles.toggle}/>
          <LayoutInfo
            layout={firstLayout}
            showValidity
            style={{float: 'right'}} />
        </div>
        <div style={commonStyles.formText}>
          {la(`
            This provides extreme performance improvements for BI queries by creating
            “invisible cubes”.
          `)}
        </div>
        <AggregateForm
          style={styles.aggregateForm}
          fields={fields}
          columns={columns}
          dataset={dataset}
          location={location}
          canSelectMeasure={false}
          canUseFieldAsBothDimensionAndMeasure={false}
        />
      </div>
    );
  }
}

const styles = {
  wrap: {
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 1,
    overflow: 'hidden'
  },
  aggregateForm: {
    flexFlow: 'column nowrap',
    flexGrow: 1
  }
};
