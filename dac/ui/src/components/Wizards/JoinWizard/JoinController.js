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
import { result } from 'lodash/object';
import uuid from 'uuid';

import ChooseDataset from 'pages/ExplorePage/components/ChooseDataset';
import CustomJoin from 'pages/ExplorePage/components/CustomJoin';
import DefaultWizardFooter from 'components/Wizards/components/DefaultWizardFooter';
import StepWizard from 'components/Wizards/components/StepWizard';
import ViewStateWrapper from 'components/ViewStateWrapper';
import { connectComplexForm, InnerComplexForm } from 'components/Forms/connectComplexForm';
import Message from 'components/Message';

import { loadExploreEntities } from 'actions/explore/dataset/get';
import { loadJoinDataset, setJoinTab, resetJoins, setJoinStep } from 'actions/explore/join';
import { loadRecommendedJoin } from 'actions/explore/join';

import { getTableColumns, getJoinTable, getExploreState } from 'selectors/explore';
import { getLocation } from 'selectors/routing';

import { RECOMMENDED_JOIN, CUSTOM_JOIN} from 'constants/explorePage/joinTabs';

import { isEmptyValue } from 'utils/validation';
import { constructFullPath } from 'utils/pathUtils';
import CancelablePromise, { Cancel } from 'utils/CancelablePromise';

import { FLEX_WRAPPER } from 'uiTheme/radium/flexStyle';

//TODO refactor recommendation logic

export const JOIN_TABLE_VIEW_ID = 'JoinTable';

@Radium
export class JoinController extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    recommendedJoins: PropTypes.instanceOf(Immutable.List),
    activeRecommendedJoin: PropTypes.instanceOf(Immutable.Map),
    viewState: PropTypes.instanceOf(Immutable.Map),
    location: PropTypes.object.isRequired,

    // callbacks
    submit: PropTypes.func,
    cancel: PropTypes.func,
    changeFormType: PropTypes.func,

    // form
    fields: PropTypes.object,
    submitting: PropTypes.bool,
    valid: PropTypes.bool,
    values: PropTypes.object,

    // connected
    joinTab: PropTypes.oneOf([RECOMMENDED_JOIN, CUSTOM_JOIN]),
    joinStep: PropTypes.oneOf([undefined, 1, 2]),
    leftColumns: PropTypes.object,
    rightColumns: PropTypes.object,

    // actions
    loadExploreEntities: PropTypes.func,
    loadJoinDataset: PropTypes.func,
    loadRecommendedJoin: PropTypes.func,
    setJoinTab: PropTypes.func,
    resetJoins: PropTypes.func,
    setJoinStep: PropTypes.func
  };

  static contextTypes = {
    router: PropTypes.object
  };

  static defaultProps = {
    recommendedJoins: Immutable.List()
  };

  state = {};

  recommendationsPromise = null; // eslint-disable-line react/sort-comp

  componentWillMount() {
    this.props.resetJoins();
    this.tryToLoadRecommendations(this.props);
  }

  componentWillReceiveProps(nextProps) {
    this.tryToLoadJoinDataset(nextProps);
    this.resetPreviewInfo(nextProps);
    this.tryToPreviewRecommendedJoin(nextProps);
  }

  componentWillUnmount() {
    if (this.recommendationsPromise) {
      this.recommendationsPromise.cancel();
    }
  }

  loadRecommendations() {
    if (this.recommendationsPromise) {
      this.recommendationsPromise.cancel();
    }
    this.recommendationsPromise = CancelablePromise.resolve(this.props.loadRecommendedJoin({
      href: `${this.props.dataset.getIn(['apiLinks', 'self'])}/join_recs`
    }));
    return this.recommendationsPromise;
  }

  resetPreviewInfo(nextProps) {
    const isJoinTabChanged = nextProps.joinTab !== this.props.joinTab;
    const { location } = nextProps;

    // we are changing tabs, so we want to reset state.  However if we choose to edit a recommendation we do not want
    // to reset since we want to carry over the recommendation information.
    // TODO: ideally the reset should be confined to the join state
    if (isJoinTabChanged && nextProps.joinStep !== 2) {
      // we reset the activeDataset and the columns that could have been set by recommended joins for example
      nextProps.fields.activeDataset.onChange('');
      nextProps.fields.columns.onChange([]);
      this.context.router.replace({
        ...location,
        state: {
          ...location.state,
          previewVersion: ''
        }
      });
    }
  }

  tryToPreviewRecommendedJoin(nextProps) {
    if (nextProps.joinTab !== RECOMMENDED_JOIN || !nextProps.recommendedJoins.size) {
      return;
    }
    const oldActiveRecommendedJoin = this.props.activeRecommendedJoin || Immutable.Map();
    const isActiveRecommendedJoinChanged = nextProps.activeRecommendedJoin.size
      && !oldActiveRecommendedJoin.equals(nextProps.activeRecommendedJoin);
    if (isActiveRecommendedJoinChanged) {
      if (isValidJoin(nextProps.values)) {
        this.submit(nextProps.values, 'preview').catch((error) => {
          this.setState({ previewError: error });
        });
      }
    }
  }

  submit = (values, ...args) => {
    if (!isValidJoin(values)) {
      return Promise.reject({_error: {
        message: la('Matching pairs of join fields are required.'),
        id: uuid.v4()
      }});
    }

    return this.props.submit(values, ...args);
  }

  cancel = () => {
    if (this.props.cancel) {
      this.props.cancel();
    }
  }

  tryToLoadRecommendations(props) {
    const { dataset } = props;
    if (dataset) {
      this.loadRecommendations()
        .then((response) => {
          if (!this.props.joinTab && !response.error) {
            const {recommendations} = response.payload;
            let nextTab = CUSTOM_JOIN;
            if (recommendations.length) {
              nextTab = RECOMMENDED_JOIN;
            }

            this.props.setJoinTab(nextTab);
          }
        })
        .catch((error) => {
          if (error instanceof Cancel) return;
          throw error;
        });
    }
  }

  tryToLoadJoinDataset(nextProps) {
    // skip if we are not on the custom join tab
    if (nextProps.joinTab !== CUSTOM_JOIN) {
      return;
    }

    const joinFullPath = constructFullPath(nextProps.values.activeDataset);
    const oldJoinFullPath = constructFullPath(this.props.values.activeDataset);
    if (joinFullPath && joinFullPath !== oldJoinFullPath) {
      nextProps.loadJoinDataset(nextProps.values.activeDataset, JOIN_TABLE_VIEW_ID);
    }
  }

  selectDataset = () => {
    this.props.setJoinStep(2);
  }

  changeFormType = (...args) => {
    this.props.setJoinStep(2);
    this.props.changeFormType(...args);
  }

  renderTab() {
    const { dataset, joinTab } = this.props;
    const datasetName = dataset && dataset.get('displayFullPath').last();
    const viewStateWrapperStyle = this.props.viewState.get('isInProgress')
      ? { position: 'relative' } : {};
    return (
      <ViewStateWrapper style={{...styles.viewStateWrapper, ...viewStateWrapperStyle}} viewState={this.props.viewState}
        hideChildrenWhenFailed={false}>
        {joinTab === RECOMMENDED_JOIN && <ChooseDataset
          currentDatasetName={datasetName}
          recommendedJoins={this.props.recommendedJoins}
          activeRecommendedJoin={this.props.activeRecommendedJoin}
          loadExploreEntities={this.props.loadExploreEntities}
          fields={this.props.fields}
          location={this.props.location}
          values={this.props.values}
          style={{flex: 1}}
        />}
        {joinTab === CUSTOM_JOIN && <CustomJoin
          tabId={CUSTOM_JOIN}
          dataset={this.props.dataset}
          fields={this.props.fields}
          location={this.props.location}
          joinStep={this.props.joinStep}
          rightColumns={this.props.rightColumns}
          leftColumns={this.props.leftColumns}
          style={{flex: 1}}
        />}
      </ViewStateWrapper>
    );
  }

  renderPreviewErrorMessage() {
    return this.state.previewError && (
      <Message
        messageType='error'
        message={this.state.previewError._error.message}
        messageId={this.state.previewError._error.id}
      />
    );
  }

  render() {
    const { joinTab } = this.props;
    const isRecommendedTabActive = joinTab === RECOMMENDED_JOIN;
    const footer = this.props.joinStep === 2 || isRecommendedTabActive
      ? (
        <DefaultWizardFooter
          onFormSubmit={this.submit}
          onCancel={this.cancel}
          submitting={this.props.submitting}
          isPreviewAvailable={!isRecommendedTabActive}
          valid={this.props.valid}
          style={{width: '100%', borderTop: 'none', padding: '0 5px', margin: 0}} />
      )
      : (
        <StepWizard
          hasActiveDataset={Boolean(this.props.fields.activeDataset.value)}
          onNextClick={this.selectDataset}
          changeFormType={this.changeFormType}
          onCancelClick={this.cancel}
          style={{flexShrink: 0}}
        />
      );

    return (
      <div className='join'>
        {this.renderPreviewErrorMessage()}
        <InnerComplexForm
          {...this.props}
          style={styles.form}
          onSubmit={this.submit}>
          <div style={FLEX_WRAPPER}>
            {this.renderTab()}
          </div>
          {footer}
        </InnerComplexForm>
      </div>
    );
  }
}

function mapStateToProps(state, ownProps) {
  const location = getLocation(state);
  const explorePageState = getExploreState(state);
  const joinDatasetPathList = explorePageState.join.getIn(['custom', 'joinDatasetPathList']);
  return {
    joinTab: explorePageState.join.get('joinTab'),
    leftColumns: getTableColumns(state, ownProps.dataset.get('datasetVersion'), location),
    rightColumns: getJoinTable(state, ownProps).get('columns'),
    joinStep: explorePageState.join.get('step'),
    initialValues: {
      joinType: 'Inner',
      activeDataset: joinDatasetPathList,
      columns: []
    }
  };
}

export default connectComplexForm({
  form: 'CustomJoin',
  fields: ['activeDataset', 'joinType', 'columns'],
  // without this option form is not valid when initialized with recommended join
  // and "Apply" button fails to submit form
  overwriteOnInitialValuesChange: false
}, [], mapStateToProps, {
  loadExploreEntities,
  loadRecommendedJoin,
  loadJoinDataset,
  setJoinTab,
  resetJoins,
  setJoinStep
})(JoinController);


const styles = {
  form: {
    flexWrap: 'none',
    flexDirection: 'column',
    height: 329
  },
  viewStateWrapper: {
    ...FLEX_WRAPPER,
    height: 'auto'
  }
};

function isValidJoin(values) {
  const value = result(values, 'columns');
  const isInvalidJoin = value.some((item) =>
    isEmptyValue(item.joinedColumn) || isEmptyValue(item.joinedTableKeyColumnName)
  );
  if (!value || value.length === 0 || isInvalidJoin) {
    return false;
  }
  return true;
}
