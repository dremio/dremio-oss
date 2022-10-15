/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { PureComponent } from "react";
import PropTypes from "prop-types";
import Immutable from "immutable";
import { connect } from "react-redux";
import ApiUtils from "utils/apiUtils/apiUtils";
import exploreUtils from "utils/explore/exploreUtils";

import TransformHeader from "pages/ExplorePage/components/Transform/TransformHeader";
import JoinHeader from "pages/ExplorePage/components/JoinTypes/JoinHeader";
import DataTypeConverterView from "pages/ExplorePage/components/DataConverter/DataTypeConverterView";
import CleanDataContent from "pages/ExplorePage/components/MixedData/CleanDataContent";

import { resetViewState } from "actions/resources";
import { JOIN_TABLE_VIEW_ID } from "components/Wizards/JoinWizard/JoinController";
import { navigateToNextDataset } from "actions/explore/dataset/common";
import {
  runTableTransform,
  transformHistoryCheck,
} from "actions/explore/dataset/transform";
import { transformPeek } from "actions/explore/dataset/peek";
import { getExploreState, getImmutableTable } from "selectors/explore";
import { getViewState } from "selectors/resources";

import { CUSTOM_JOIN } from "@app/constants/explorePage/joinTabs";
import { getActiveScript } from "@app/selectors/scripts";
import apiUtils from "utils/apiUtils/apiUtils";
import {
  setQuerySelections,
  setQueryStatuses,
  setPreviousAndCurrentSql,
} from "@app/actions/explore/view";
import {
  fetchFilteredJobsList,
  JOB_PAGE_NEW_VIEW_ID,
} from "@app/actions/joblist/jobList";
import { extractSelections } from "@app/utils/statements/statementParser";
import CalculatedFieldContent from "./DetailsWizard/CalculatedFieldContent";
import TransformContent from "./DetailsWizard/TransformContent";
import ConvertTrimContent from "./DetailsWizard/ConvertTrimContent";
import SortMultiplyContent from "./DetailsWizard/SortMultiplyContent";
import RawHeader from "./components/RawHeader";
import GroupByController from "./GroupByWizard/GroupByController";
import JoinController from "./JoinWizard/JoinController";
import { base } from "./DetailsWizard.less";

export const RECOMMENDED_JOINS_VIEW_ID = "RecommendedJoins";

export class DetailsWizard extends PureComponent {
  static propTypes = {
    detailType: PropTypes.string,
    dataset: PropTypes.instanceOf(Immutable.Map),
    tableData: PropTypes.instanceOf(Immutable.Map),
    location: PropTypes.object.isRequired,
    navigateToNextDataset: PropTypes.func,
    runTableTransform: PropTypes.func,
    transformHistoryCheck: PropTypes.func,
    transformPeek: PropTypes.func,
    sqlSize: PropTypes.number,
    resetViewState: PropTypes.func,
    recommendedJoins: PropTypes.instanceOf(Immutable.List),
    activeRecommendedJoin: PropTypes.instanceOf(Immutable.Map),
    recommendedJoinsViewState: PropTypes.instanceOf(Immutable.Map),
    joinTableViewState: PropTypes.instanceOf(Immutable.Map),
    dragType: PropTypes.string,
    exploreViewState: PropTypes.instanceOf(Immutable.Map).isRequired,
    canSelect: PropTypes.any,
    activeScript: PropTypes.object,
    queryTabNumber: PropTypes.number,
    queryStatuses: PropTypes.array,
    setQuerySelections: PropTypes.func,
    setQueryStatuses: PropTypes.func,
    setPreviousAndCurrentSql: PropTypes.func,
    fetchFilteredJobsList: PropTypes.func,
  };

  static contextTypes = {
    router: PropTypes.object,
  };

  constructor(props) {
    super(props);
    this.submit = this.submit.bind(this);
    this.state = {
      form: null,
    };
  }

  getViewId() {
    return this.props.exploreViewState.get("viewId");
  }

  getJoinViewState() {
    return this.props.location.query.joinTab === CUSTOM_JOIN
      ? this.props.joinTableViewState
      : this.props.recommendedJoinsViewState;
  }

  goToExplorePage() {
    const { location, activeScript } = this.props;
    const sliceIndex = location.pathname.indexOf("/details");
    const newPath = location.pathname.slice(0, sliceIndex);
    const { type, ...queryParams } = location.query || {}; // eslint-disable-line

    this.context.router.push({
      state: {
        previewVersion: "",
        renderScriptTab: !!activeScript.id,
      },
      pathname: newPath,
      query: queryParams,
    });
  }

  handleFormTypeChange = (formType) => this.setState({ formType });
  handleCancelClick = () => {
    this.props.resetViewState(this.getViewId());
    this.goToExplorePage();
  };

  handleTransformPeek = (values, submitType) => {
    const { dataset, detailType } = this.props;
    return this.props.transformPeek(
      dataset,
      values,
      detailType,
      this.getViewId(),
      submitType
    );
  };

  handleSuccessfulSubmit = (response) => {
    const {
      queryTabNumber,
      queryStatuses,
      setPreviousAndCurrentSql: newPreviousAndCurrentSql,
      setQuerySelections: newQuerySelections,
      setQueryStatuses: newQueryStatuses,
      fetchFilteredJobsList,
    } = this.props;

    const [sqlStatement, jobId, , version] = apiUtils.getFromResponse(response);

    let modifiedQuery = "";
    for (let i = 0; i < queryStatuses.length; i++) {
      if (i === queryTabNumber - 1) {
        modifiedQuery += sqlStatement + ";\n";

        const mostRecentStatuses = [...queryStatuses];
        mostRecentStatuses[i] = {
          sqlStatement,
          version,
          jobId,
          cancelled: false,
        };
        newQueryStatuses({ statuses: mostRecentStatuses });
        fetchFilteredJobsList(jobId, JOB_PAGE_NEW_VIEW_ID, i);
      } else {
        modifiedQuery += queryStatuses[i].sqlStatement + ";\n";
      }
    }

    newPreviousAndCurrentSql({ sql: modifiedQuery });
    const newSelections = extractSelections(modifiedQuery);
    newQuerySelections({ selections: newSelections });
  };

  handleApply = (values) => {
    const { dataset, detailType, tableData, activeScript } = this.props;

    return this.props
      .runTableTransform(
        dataset,
        exploreUtils.getMappedDataForTransform(values, detailType),
        this.getViewId(),
        tableData
      )
      .then((response) => {
        if (!response.error) {
          this.handleSuccessfulSubmit(response);
          // this navigation will trigger data load. see explorePageDataChecker saga
          return this.props.navigateToNextDataset(response, {
            renderScriptTab: !!activeScript.id,
            isTransform: true,
          });
        }
        return response;
      });
  };

  submit(values, submitType) {
    const { detailType } = this.props;
    const curType =
      typeof submitType === "string" ? submitType : this.state.formType;
    const action =
      curType === "apply" ? this.handleApply : this.handleTransformPeek;
    const { columnType, columnName } = this.props.location.state || {};
    const val =
      detailType === "transform"
        ? values
        : { ...values, columnType, columnName };

    if (curType === "autoPeek") {
      const promise = action(val, curType);

      return promise;
    }
    if (curType === "apply") {
      return new Promise((resolve, reject) => {
        this.props.transformHistoryCheck(
          this.props.dataset,
          () => {
            const promise = action(val, curType);
            ApiUtils.attachFormSubmitHandlers(promise)
              .then(resolve)
              .catch((error) => {
                reject(error);
              });
          },
          () => {
            resolve();
          }
        );
      });
    }
    // User clicked Preview
    const promise = action(val, curType);
    return ApiUtils.attachFormSubmitHandlers(promise);
  }

  renderHeader() {
    const defaultProps = {
      separator: " ",
      closeIcon: true,
      closeIconHandler: this.handleCancelClick,
    };
    const { location, detailType } = this.props;

    switch (detailType) {
      case "JOIN":
        return (
          <JoinHeader
            viewState={this.getJoinViewState()}
            hasRecommendations={
              this.props.recommendedJoins &&
              this.props.recommendedJoins.size > 0
            }
            {...defaultProps}
          />
        );
      case "CONVERT_CASE":
        return <RawHeader text="Convert Case" {...defaultProps} />;
      case "MULTIPLE":
        return <RawHeader text="Sort Multiple" {...defaultProps} />;
      case "TRIM_WHITE_SPACES":
        return <RawHeader text="Trim Whitespace" {...defaultProps} />;
      case "CALCULATED_FIELD":
        return <RawHeader text="Add Calculated Field" {...defaultProps} />;
      case "CONVERT_DATA_TYPE":
        return <RawHeader text="Change Data Type" {...defaultProps} />;
      case "SINGLE_DATA_TYPE":
        return <RawHeader text="Clean Data" {...defaultProps} />;
      case "SPLIT_BY_DATA_TYPE":
        return <RawHeader text="Clean Data" {...defaultProps} />;
      case "GROUP_BY":
        return <RawHeader text="Group By" {...defaultProps} />;
      case "transform":
        return <TransformHeader location={location} {...defaultProps} />;
      default:
        return;
    }
  }

  renderContent() {
    const { columnName, columnType, props } = this.props.location.state || {};
    const { canSelect } = this.props;
    const defaultProps = {
      submit: this.submit,
      changeFormType: this.handleFormTypeChange,
      cancel: this.handleCancelClick,
      sqlSize: this.props.sqlSize,
      columnName,
      columnType,
      ...props,
    };
    const { location, detailType } = this.props;
    const locationState = location.state || {};
    switch (detailType) {
      case "JOIN":
        return (
          <JoinController
            viewState={this.getJoinViewState()}
            dataset={this.props.dataset}
            recommendedJoins={this.props.recommendedJoins}
            activeRecommendedJoin={this.props.activeRecommendedJoin}
            location={location}
            canSelect={canSelect}
            {...defaultProps}
          />
        );
      case "CONVERT_CASE":
        return (
          <ConvertTrimContent
            type="converCase"
            dataset={this.props.dataset}
            {...defaultProps}
          />
        );
      case "TRIM_WHITE_SPACES":
        return (
          <ConvertTrimContent
            type="trim"
            dataset={this.props.dataset}
            {...defaultProps}
          />
        );
      case "CALCULATED_FIELD":
        return (
          <CalculatedFieldContent
            columns={this.props.tableData.get("columns")}
            dragType={this.props.dragType}
            {...defaultProps}
          />
        );
      case "MULTIPLE":
        return (
          <SortMultiplyContent
            dataset={this.props.dataset}
            columns={this.props.tableData.get("columns")}
            location={location}
            {...defaultProps}
          />
        );
      case "CONVERT_DATA_TYPE":
        return (
          <DataTypeConverterView
            toType={locationState.toType}
            fromType={locationState.columnType}
            {...defaultProps}
          />
        );
      case "SINGLE_DATA_TYPE":
      case "SPLIT_BY_DATA_TYPE":
        return (
          <CleanDataContent
            dataset={this.props.dataset}
            detailType={detailType}
            location={location}
            {...defaultProps}
          />
        );
      case "GROUP_BY":
        return (
          <GroupByController
            dataset={this.props.dataset}
            columns={this.props.tableData.get("columns")}
            canSelect={canSelect}
            {...defaultProps}
          />
        );
      case "transform":
        return (
          <TransformContent
            dataset={this.props.dataset}
            location={location}
            {...defaultProps}
          />
        );
      default:
        return;
    }
  }

  render() {
    const { detailType } = this.props;
    const hash = {
      multiply: 251,
    };
    const height = hash[detailType] || "100%";

    return (
      <div className={base}>
        {this.renderHeader()}
        <div style={{ height }}>{this.renderContent()}</div>
      </div>
    );
  }
}

function mapStateToProps(state, props) {
  const location = state.routing.locationBeforeTransitions || {};
  const explorePageState = getExploreState(state);
  return {
    detailType: location.query.type,
    tableData: getImmutableTable(
      state,
      props.dataset.get("datasetVersion"),
      location
    ),
    sqlSize: explorePageState.ui.get("sqlSize"),
    recommendedJoins:
      explorePageState.join.getIn(["recommended", "recommendedJoins"]) ||
      Immutable.List([]),
    activeRecommendedJoin:
      explorePageState.join.getIn(["recommended", "activeRecommendedJoin"]) ||
      Immutable.Map(),
    recommendedJoinsViewState: getViewState(state, RECOMMENDED_JOINS_VIEW_ID),
    joinTableViewState: getViewState(state, JOIN_TABLE_VIEW_ID),
    activeScript: getActiveScript(state),
    queryTabNumber: explorePageState.view.queryTabNumber,
    queryStatuses: explorePageState.view.queryStatuses,
  };
}

export default connect(mapStateToProps, {
  navigateToNextDataset,
  runTableTransform,
  transformHistoryCheck,
  transformPeek,
  resetViewState,
  setQuerySelections,
  setQueryStatuses,
  setPreviousAndCurrentSql,
  fetchFilteredJobsList,
})(DetailsWizard);
