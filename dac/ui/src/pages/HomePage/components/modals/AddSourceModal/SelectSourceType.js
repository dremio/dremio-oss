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
import { Component } from "react";
import { sortBy } from "lodash/collection";
import { injectIntl } from "react-intl";
import PropTypes from "prop-types";

import SelectConnectionButton from "components/SelectConnectionButton";
import { sourceTypesIncludeS3 } from "utils/sourceUtils";
import {
  isDatabaseType,
  isMetastoreSourceType,
  isDataPlaneSourceType,
} from "@app/constants/sourceTypes.js";
import { isDataPlaneEnabled } from "@inject/utils/dataPlaneUtils";
import "pages/HomePage/components/modals/AddSourceModal/SelectSourceType.less";
import SearchSource from "./SearchSource";

@injectIntl
export default class SelectSourceType extends Component {
  constructor(props) {
    super(props);
    this.state = {
      filteredSourceTypes: null,
      searchString: null,
    };
    this.updateSourcesList = this.updateSourcesList.bind(this);
  }

  static propTypes = {
    onSelectSource: PropTypes.func,
    sourceTypes: PropTypes.array,
    intl: PropTypes.object.isRequired,
    isExternalSource: PropTypes.bool,
    isDataPlaneSource: PropTypes.bool,
  };

  getEnabledSourceTypes(allTypes) {
    return sortBy(
      allTypes.filter((type) => !type.disabled),
      ["label"]
    );
  }

  getDisabledSourceTypes(allTypes) {
    return sortBy(
      allTypes.filter((type) => type.disabled),
      ["enterprise", "label"]
    );
  }

  updateSourcesList(filteredSourceTypes, searchString) {
    this.setState({
      filteredSourceTypes,
      searchString,
    });
  }

  renderSearchBox() {
    const { sourceTypes } = this.props;
    return (
      <SearchSource
        sources={sourceTypes}
        updateSources={this.updateSourcesList}
      />
    );
  }

  renderSourceTypes(connections) {
    const { intl } = this.props;
    return connections.map((item) => {
      let pillText = "";
      let isCommunity = false;
      if (item.disabled) {
        pillText = intl.formatMessage({ id: "Source.CommingSoonTag" });
      } else if (item.tags && item.tags.includes("beta")) {
        pillText = intl.formatMessage({ id: "Source.BetaTag" });
      } else if (item.tags && item.tags.includes("community")) {
        pillText = intl.formatMessage({ id: "Source.CommunityTag" });
        isCommunity = true;
      }

      return (
        <SelectConnectionButton
          label={item.label}
          pillText={pillText}
          isCommunity={isCommunity}
          disabled={item.disabled}
          dremioIcon={`sources/${item.sourceType}`}
          key={item.sourceType}
          onClick={
            !item.disabled
              ? this.props.onSelectSource.bind(this, item)
              : undefined
          }
        />
      );
    });
  }

  renderSampleSource() {
    return (
      <SelectConnectionButton
        sampleSource
        label={this.props.intl.formatMessage({ id: "Source.SampleSource" })}
        dremioIcon="sources/SampleSource"
        onClick={this.props.onSelectSource.bind(this, {
          sourceType: "SampleSource",
        })}
      />
    );
  }

  renderExternalSources() {
    const { sourceTypes, intl } = this.props;
    const { filteredSourceTypes } = this.state;
    const sources = filteredSourceTypes || sourceTypes;
    const externalSources = sources.filter(
      (source) =>
        isDatabaseType(source.sourceType) &&
        !isDataPlaneSourceType(source.sourceType)
    );
    return (
      externalSources.length > 0 && (
        <div className="SelectSourceType">
          <div className="main">
            <div className="source-type-section">
              <div className="source-type-header">
                {intl.formatMessage({ id: "Source.Databases" })}
              </div>
              {this.renderSourceTypes(
                this.getEnabledSourceTypes(externalSources)
              )}
            </div>
            <div className="source-type-section">
              {this.renderSourceTypes(
                this.getDisabledSourceTypes(externalSources)
              )}
            </div>
          </div>
        </div>
      )
    );
  }

  renderDataPlanSources() {
    const { sourceTypes, intl } = this.props;
    const { filteredSourceTypes } = this.state;
    const sources = filteredSourceTypes || sourceTypes;
    const dataPlaneSources = sources.filter((source) =>
      isDataPlaneSourceType(source.sourceType)
    );
    return (
      dataPlaneSources.length > 0 && (
        <div className="SelectSourceType">
          <div className="main">
            <div className="source-type-section">
              <div className="source-type-header">
                {intl.formatMessage({ id: "Source.Nessie" })}
              </div>
              {this.renderSourceTypes(
                this.getEnabledSourceTypes(dataPlaneSources)
              )}
              {this.renderSourceTypes(
                this.getDisabledSourceTypes(dataPlaneSources)
              )}
            </div>
          </div>
        </div>
      )
    );
  }

  renderDataLakeSources() {
    const { sourceTypes, intl } = this.props;
    const { filteredSourceTypes, searchString } = this.state;
    const sources = filteredSourceTypes || sourceTypes;
    const fileStoreSources = sources.filter(
      (source) =>
        !isDatabaseType(source.sourceType) &&
        !isMetastoreSourceType(source.sourceType) &&
        !isDataPlaneSourceType(source.sourceType)
    );
    const tableStoreSources = sources.filter((source) =>
      isMetastoreSourceType(source.sourceType)
    );
    const sampleSource = "Sample Source";
    const renderSampleSource = filteredSourceTypes
      ? sampleSource.toLowerCase().indexOf(searchString) > -1
      : true;

    return (
      (fileStoreSources.length > 0 ||
        tableStoreSources.length > 0 ||
        renderSampleSource) && (
        <div className="SelectSourceType">
          <div className="main">
            {tableStoreSources.length > 0 && (
              <div className="source-type-section">
                <div className="source-type-header">
                  {intl.formatMessage({ id: "Source.MetaStores" })}
                </div>
                {this.renderSourceTypes(
                  this.getEnabledSourceTypes(tableStoreSources)
                )}
                {this.renderSourceTypes(
                  this.getDisabledSourceTypes(tableStoreSources)
                )}
              </div>
            )}
            {(fileStoreSources.length > 0 || renderSampleSource) && (
              <div className="source-type-section">
                <div className="source-type-header">
                  {intl.formatMessage({ id: "Source.ObjectStorage" })}
                </div>
                {this.renderSourceTypes(
                  this.getEnabledSourceTypes(fileStoreSources)
                )}
                {sourceTypesIncludeS3(sourceTypes) &&
                  renderSampleSource &&
                  this.renderSampleSource()}
                {this.renderSourceTypes(
                  this.getDisabledSourceTypes(fileStoreSources)
                )}
              </div>
            )}
          </div>
        </div>
      )
    );
  }

  render() {
    return (
      <>
        {this.renderSearchBox()}
        {isDataPlaneEnabled && this.renderDataPlanSources()}
        {this.renderDataLakeSources()}
        {this.renderExternalSources()}
      </>
    );
  }
}
