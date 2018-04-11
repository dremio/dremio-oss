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
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import Tabs from 'components/Tabs';
import { Radio } from 'components/Fields';
import { formLabel } from 'uiTheme/radium/typography';
import { inlineLabel, inlineFieldWrap } from 'uiTheme/radium/exploreTransform';

import ExtractSingleList from './../../cardContent/ExtractSingleList';
import ExtractMultipleList from './../../cardContent/ExtractMultipleList';
import TransformCard from './../../TransformCard';

@Radium
export default class ExtractListCard extends Component {

  static getFields() {
    return [
      ...ExtractSingleList.getFields(),
      ...ExtractMultipleList.getFields(),
      'type'
    ];
  }

  static validate(values) {
    if (values.type === 'single') {
      return ExtractSingleList.validate(values);
    } else if (values.type === 'multiple') {
      return ExtractMultipleList.validate(values);
    }
    return {
      type: 'Type is required.'
    };
  }

  static propTypes = {
    card: PropTypes.instanceOf(Immutable.Map),
    fields: PropTypes.object,
    active: PropTypes.bool,
    onClick: PropTypes.func
  };

  renderBack() {
    const { fields } = this.props;
    return (
      <div>
        <div style={inlineFieldWrap}>
          <span style={inlineLabel}>
            Extract
          </span>
          <Radio
            {...fields.type}
            radioValue='single'
            label='Single'
            style={{...formLabel, ...styles.radio}}/>
          <Radio
            {...fields.type}
            radioValue='multiple'
            label='Multiple'
            style={{...formLabel, ...styles.radio}}/>
        </div>
        <div className='transform-card-content'>
          <Tabs activeTab={fields.type.value}>
            <ExtractSingleList
              tabId='single'
              startIndex={fields.single.startIndex}
              firstElementLabel='Element'/>
            <ExtractMultipleList
              tabId='multiple'
              startIndex={fields.multiple.startIndex}
              endIndex={fields.multiple.endIndex}
              firstElementLabel='First Element'
              lastElementLabel='Last Element'/>
          </Tabs>
        </div>
      </div>
    );
  }

  render() {
    const { card, active, onClick } = this.props;
    return (
      <TransformCard
        back={this.renderBack()}
        card={card}
        active={active}
        onClick={onClick}
        editing
      />
    );
  }
}

export const styles = {
  header: {
    marginLeft: 10,
    marginTop: 10,
    ...formLabel
  },
  radio: {
    width: 75
  },
  cardStyle: {
    display: 'flex',
    flexDirection: 'column',
    marginBottom: 20
  },
  title: {
    ...formLabel,
    margin: 10
  }
};
