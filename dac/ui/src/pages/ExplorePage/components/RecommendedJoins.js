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
import Radium from 'radium';
import PropTypes from 'prop-types';
import Immutable from 'immutable';

import datasetSchema from 'schemas/v2/fullDataset';
import { setActiveRecommendedJoin, resetActiveRecommendedJoin, editRecommendedJoin } from 'actions/explore/join';

import Spinner from 'components/Spinner';

import CancelablePromise, { Cancel } from 'utils/CancelablePromise';
import {RECOMMENDED_JOINS_VIEW_ID} from 'components/Wizards/DetailsWizard';
import RecommendedJoinItem from './RecommendedJoinItem';

@Radium
export class RecommendedJoins extends Component {
  static propTypes = {
    recommendedJoins: PropTypes.instanceOf(Immutable.List),
    activeRecommendedJoin: PropTypes.instanceOf(Immutable.Map),
    location: PropTypes.object,
    loadExploreEntities: PropTypes.func,
    fields: PropTypes.object,
    setActiveRecommendedJoin: PropTypes.func,
    resetActiveRecommendedJoin: PropTypes.func,
    editRecommendedJoin: PropTypes.func
  };

  static contextTypes = {
    router: PropTypes.object.isRequired
  };

  loadTablePromise = null; // eslint-disable-line react/sort-comp

  constructor(props) {
    super(props);
    this.selectJoin = this.selectJoin.bind(this);
  }

  componentDidMount() {
    if (this.props.recommendedJoins && this.props.recommendedJoins.size) {
      this.selectJoin(this.props.recommendedJoins.first(), false);
    }
  }

  componentWillUnmount() {
    this.props.resetActiveRecommendedJoin();
    if (this.loadTablePromise) this.loadTablePromise.cancel();
  }

  loadRecommendedTable(recommendation) {
    const uiPropsForEntity = [{key: 'version', value: this.getUniqRecommendationId(recommendation)}];
    const href = recommendation && recommendation.get && recommendation.get('links') &&
    recommendation.getIn(['links', 'data']);

    if (this.loadTablePromise) {
      this.loadTablePromise.cancel();
    }
    this.loadTablePromise = CancelablePromise.resolve(
      this.props.loadExploreEntities({href, schema: datasetSchema, viewId: RECOMMENDED_JOINS_VIEW_ID, uiPropsForEntity})
    );
    return this.loadTablePromise;
  }

  getUniqRecommendationId(recommendation) {
    const fullPathListJSON = recommendation.get('rightTableFullPathList').toJSON();
    const stringifiedFullPathListJSON = fullPathListJSON && fullPathListJSON.join && fullPathListJSON.join('.');
    return stringifiedFullPathListJSON + recommendation.get('joinType');
  }

  updateFormFields(recommendation) {
    const { fields: { activeDataset, joinType, columns }} = this.props;
    activeDataset.onChange(recommendation.get('rightTableFullPathList').toJS());
    joinType.onChange(recommendation.get('joinType'));
    const matchingKeys = recommendation.get('matchingKeys');
    columns.onChange(matchingKeys.keySeq().map(leftKey => (
      { joinedTableKeyColumnName: matchingKeys.get(leftKey), joinedColumn: leftKey }
    )).toJS());
  }

  selectJoin(recommendation, isCustom) {
    if (isCustom) {
      // if we are a custom join (editing a recommendation), we need to load the recommendation entity information
      // before we proceed.
      this.updateFormFields(recommendation);

      this.loadRecommendedTable(recommendation).then((response) => {
        const joinVersion = response.payload.get('result');
        // joinVersion is used to load the join entity data, so we set it here
        this.props.editRecommendedJoin(recommendation, joinVersion);
      }).catch((error) => {
        if (error instanceof Cancel) return;
        throw error;
      });

      return;
    }

    if (!recommendation.equals(this.props.activeRecommendedJoin)) {
      this.props.setActiveRecommendedJoin(recommendation);
      this.updateFormFields(recommendation);
    }
  }

  renderDatasets() {
    if (!this.props.activeRecommendedJoin.size) {
      return <Spinner/>;
    }

    return this.props.recommendedJoins && this.props.recommendedJoins.map((recommendation, i) => {
      return (
        <RecommendedJoinItem
          key={i}
          recommendation={recommendation}
          isActive={recommendation.equals(this.props.activeRecommendedJoin)}
          selectJoin={this.selectJoin}
          fields={this.props.fields}/>
      );
    });
  }

  render() {
    return (
      <div className='recommended-joins' style={styles.base}>
        {this.renderDatasets()}
      </div>
    );
  }
}

export default connect(null, {
  setActiveRecommendedJoin,
  resetActiveRecommendedJoin,
  editRecommendedJoin
})(RecommendedJoins);

const styles = {
  base: {
    maxHeight: 235,
    minHeight: 235,
    overflow: 'auto',
    marginLeft: 20,
    marginRight: 20,
    borderBottom: '1px solid #ccc',
    borderTop: '1px solid #ccc',
    borderLeft: '1px solid #ccc',
    borderRight: '1px solid #ccc',
    position: 'relative' // use relative so that the spinner only covers us
  }
};
