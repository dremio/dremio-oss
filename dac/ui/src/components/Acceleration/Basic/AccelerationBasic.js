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
import { connect } from 'react-redux';
import Radium from 'radium';
import Immutable from 'immutable';
import FontIcon from 'components/Icon/FontIcon';
import { FLEX_COL_START } from 'uiTheme/radium/flexStyle';
import { pathLink, body } from 'uiTheme/radium/typography';
import { modalFormProps } from 'components/Forms';
import { Toggle } from 'components/Fields';
import { commonStyles } from '../commonStyles';
import LayoutInfo from '../LayoutInfo';
import AccelerationAggregate from './AccelerationAggregate';

@Radium
export class AccelerationBasic extends Component {
  static getFields() {
    return AccelerationAggregate.getFields();
  }
  static validate(values) {
    return AccelerationAggregate.validate(values);
  }
  static propTypes = {
    acceleration: PropTypes.instanceOf(Immutable.Map).isRequired,
    location: PropTypes.object.isRequired,
    fullPath: PropTypes.string,
    fields: PropTypes.object,
    handleSubmit: PropTypes.func,
    submit: PropTypes.func,
    onCancel: PropTypes.func
  };

  static defaultProps = {
    fields: {}
  };

  getHighlightedSection() {
    const {layoutId} = (this.props.location.state || {});
    if (!layoutId) return null;

    let found = this.props.acceleration.getIn([
      'aggregationLayouts', 'layoutList'
    ]).some(layout => layout.getIn(['id', 'id']) === layoutId);

    if (found) return 'AGGREGATION';

    found = this.props.acceleration.getIn([
      'rawLayouts', 'layoutList'
    ]).some(layout => layout.getIn(['id', 'id']) === layoutId);

    if (found) return 'RAW';

    return null;
  }

  render() {
    const { fields, location, fullPath, acceleration } = this.props;
    const { enabled } = fields.rawLayouts || {};
    const toggleLabel = (
      <div style={commonStyles.toggleLabel}>
        <FontIcon type='RawMode' theme={commonStyles.iconTheme}/>
        {la('Raw Reflections (Automatic)')}
      </div>
    );

    const firstRawLayout = acceleration.getIn('rawLayouts.layoutList.0'.split('.'));
    const highlightedSection = this.getHighlightedSection();

    return (
      <div style={styles.wrap}>
        <div style={{...styles.formSection, marginBottom: 15}}>
          <div style={{
            ...commonStyles.header,
            ...(highlightedSection === 'RAW' ? commonStyles.highlight : {})
          }} data-qa='raw-queries-toggle' >
            <Toggle {...enabled} label={toggleLabel} style={commonStyles.toggle}/>
            <LayoutInfo
              layout={firstRawLayout}
              showValidity
              style={{float: 'right'}} />
          </div>
          <div style={{...commonStyles.formText, padding: 10}}>
            {la(`
               This provides enhanced performance for slow data sources and file formats
               and ensures that analytical queries don't impact the data source, by creating
               columnar partitioned materializations.
               `)}
          </div>
        </div>
        <AccelerationAggregate
          {...modalFormProps(this.props)}
          acceleration={acceleration}
          fields={fields}
          style={styles.formSection}
          location={location}
          fullPath={fullPath}
          shouldHighlight={highlightedSection === 'AGGREGATION'}
        />
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

export default connect(mapStateToProps)(AccelerationBasic);


const styles = {
  wrap: {
    ...FLEX_COL_START,
    width: '100%',
    height: '100%',
    position: 'relative',
    overflow: 'hidden'
  },
  body: {
    ...body,
    ...FLEX_COL_START,
    height: '100%',
    paddingLeft: 10
  },
  link: {
    ...pathLink,
    margin: '2px 0 0 10px',
    ':hover': {cursor: 'pointer'}
  },
  header: {
    margin: '10px 0',
    display: 'flex',
    alignItems: 'center'
  },
  column: {
    ...body,
    height: 30
  },
  formSection: {
    backgroundColor: '#f3f3f3',
    border: '1px solid #e1e1e1'
  }
};
