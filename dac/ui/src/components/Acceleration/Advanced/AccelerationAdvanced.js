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
import { connect } from 'react-redux';
import Immutable from 'immutable';
import Radium from 'radium';
import PropTypes from 'prop-types';

import { formLabel } from 'uiTheme/radium/typography';
import { WHITE, CELL_EXPANSION_HEADER } from 'uiTheme/radium/colors';
import AccelerationAggregation from './AccelerationAggregation';
import AccelerationRaw from './AccelerationRaw';

@Radium
export class AccelerationAdvanced extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    reflections: PropTypes.instanceOf(Immutable.Map).isRequired,
    fields: PropTypes.object.isRequired,
    location: PropTypes.object.isRequired,
    updateFormDirtyState: PropTypes.func.isRequired,
    values: PropTypes.object.isRequired
  };

  static getFields() {
    return [
      ...AccelerationAggregation.getFields(),
      ...AccelerationRaw.getFields()
    ];
  }

  static validate(values) {
    return {
      ...AccelerationAggregation.validate(values),
      ...AccelerationRaw.validate(values)
    };
  }

  state = {
    activeTab: null
  }

  initialReflections = null;

  constructor(props) {
    super(props);

    this.initialReflections = Immutable.fromJS({
      aggregationReflections: this.props.values.aggregationReflections,
      rawReflections: this.props.values.rawReflections
    });
  }

  componentWillReceiveProps(nextProps) {
    const { values, updateFormDirtyState } = nextProps;
    const aggregationReflections = Immutable.fromJS(values.aggregationReflections);
    const rawReflections = Immutable.fromJS(values.rawReflections);

    updateFormDirtyState(!this.areAdvancedReflectionsFieldsEqual(aggregationReflections, rawReflections));
  }

  getActiveTab() {
    if (this.state.activeTab) return this.state.activeTab;

    const {layoutId} = (this.props.location.state || {});
    if (!layoutId) return 'RAW';

    const found = this.props.values.aggregationReflections.some(reflection => reflection.id === layoutId);

    return found ? 'AGGREGATION' : 'RAW';
  }

  areAdvancedReflectionsFieldsEqual(aggregationReflections, rawReflections) {
    // tracks field's dirty state because of issue in redux-form
    // we need to check dirty state differently since currently we handle array fields at 1 level deep
    // because of fields data come in random order we need to sort them to check dirty state,
    // only exception is sortFields in this case we need to keep order
    const sortByName = (arr) => arr.sortBy((value) => value.get('name'));
    const areEnabledFieldEqual = (layoutGroup, layoutName) => {
      return layoutGroup.get('enabled') === this.initialReflections.getIn([layoutName, 'enabled']);
    };

    if (!areEnabledFieldEqual(aggregationReflections, 'aggregationReflections')
      || !areEnabledFieldEqual(rawReflections, 'rawReflections')) {
      return false;
    }

    const areLayoutListEqual = (layoutList, layoutListName) => {
      return !layoutList.some((layoutListValue, i) => {
        if (!this.initialReflections.getIn([layoutListName, i])) return true;

        const currentLayoutDetails = layoutListValue;
        const initialLayoutDetails = this.initialReflections.getIn([layoutListName, i]);

        return currentLayoutDetails.some((layoutDetails, layoutDetailsName) => {
          if (!Immutable.Iterable.isIterable(layoutDetails)) {
            return layoutDetails !== initialLayoutDetails.get(layoutDetailsName);
          }

          if (layoutDetailsName === 'sortFields') {
            return !layoutDetails.equals(initialLayoutDetails.get(layoutDetailsName));
          }

          return !sortByName(layoutDetails).equals(sortByName(initialLayoutDetails.get(layoutDetailsName)));
        });
      });
    };

    return areLayoutListEqual(aggregationReflections, 'aggregationReflections')
      && areLayoutListEqual(rawReflections, 'rawReflections');
  }

  renderTableQueries() {
    const { fields, reflections, dataset } = this.props;
    return this.getActiveTab() === 'AGGREGATION'
      ? <AccelerationAggregation reflections={reflections} dataset={dataset} fields={fields}/>
      : <AccelerationRaw reflections={reflections} dataset={dataset} fields={fields}/>;
  }

  render() {
    const activeTab = this.getActiveTab();
    return (
      <div data-qa='acceleration-advanced' style={styles.base}>
        <div style={styles.tabs}>
          <div
            data-qa='raw-queries-tab'
            style={[styles.tab, {backgroundColor: activeTab === 'RAW' ? CELL_EXPANSION_HEADER : WHITE }]}
            key='raw'
            onClick={() => this.setState({ activeTab: 'RAW' })}
          >
            {la('Raw Reflections')}
          </div>
          <div
            data-qa='aggregation-queries-tab'
            style={[styles.tab, {backgroundColor: activeTab === 'AGGREGATION' ? CELL_EXPANSION_HEADER : WHITE }]}
            key='aggregation'
            onClick={() => this.setState({ activeTab: 'AGGREGATION' })}
          >
            {la('Aggregation Reflections')}
          </div>
        </div>
        {this.renderTableQueries()}
      </div>
    );
  }
}

const mapStateToProps = state => {
  const location = state.routing.locationBeforeTransitions;
  return {
    location
  };
};

export default connect(mapStateToProps)(AccelerationAdvanced);

const styles = {
  base: {
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 1
  },
  tabs: {
    display: 'flex',
    alignItems: 'center',
    height: 40,
    marginBottom: 5,
    width: '100%'
  },
  tab: {
    ...formLabel,
    height: 25,
    padding: 5,
    marginRight: 10,
    alignContent: 'center',
    ':hover': {
      backgroundColor: CELL_EXPANSION_HEADER,
      cursor: 'pointer'
    }
  }
};
