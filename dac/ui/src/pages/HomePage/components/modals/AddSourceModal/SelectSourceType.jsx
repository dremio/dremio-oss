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
import {
  sourceTypesIncludeS3,
  sourceTypesIncludeSampleSource,
} from "@inject/utils/sourceUtils";

import {
  isDatabaseType,
  isMetastoreSourceType,
  isDataPlaneSourceType,
  isLakehouseSourceType,
  isVersionedSoftwareSource,
  AZURE_SAMPLE_SOURCE,
} from "@inject/constants/sourceTypes";
import { isNotSoftware } from "dyn-load/utils/versionUtils";
import "pages/HomePage/components/modals/AddSourceModal/SelectSourceType.less";
import SearchSource from "./SearchSource";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import VENDORS from "@inject/constants/vendors";

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
      ["label"],
    );
  }

  getDisabledSourceTypes(allTypes) {
    return sortBy(
      allTypes.filter((type) => type.disabled),
      ["enterprise", "label"],
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
          isSampleDB={item.sourceType === "SAMPLEDB"}
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
    const isAzureProject =
      getSonarContext()?.getProjectVendorType?.() === VENDORS.AZURE;
    const externalSources = sources.filter(
      (source) =>
        isDatabaseType(source.sourceType) &&
        !isDataPlaneSourceType(source.sourceType) &&
        !(isAzureProject && source.sourceType === AZURE_SAMPLE_SOURCE),
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
                this.getEnabledSourceTypes(externalSources),
              )}
            </div>
            <div className="source-type-section">
              {this.renderSourceTypes(
                this.getDisabledSourceTypes(externalSources),
              )}
            </div>
          </div>
        </div>
      )
    );
  }

  renderLakehouseSources() {
    const { sourceTypes, intl } = this.props;
    const { filteredSourceTypes, searchString } = this.state;
    const sources = filteredSourceTypes || sourceTypes;
    const isAzureProject = isNotSoftware()
      ? getSonarContext()?.getProjectVendorType?.() === VENDORS?.AZURE
      : false;
    const fileStoreSources = sources.filter(
      (source) =>
        !isDatabaseType(source.sourceType) &&
        !isMetastoreSourceType(source.sourceType) &&
        !isLakehouseSourceType(source.sourceType) &&
        !isVersionedSoftwareSource(source.sourceType) &&
        !isDataPlaneSourceType(source.sourceType) &&
        !(isAzureProject && source.sourceType === AZURE_SAMPLE_SOURCE),
    );
    const lakehouseSources = sources.filter((source) =>
      isLakehouseSourceType(source.sourceType),
    );

    const sampleSource = "Sample Source";
    const renderSampleSource = filteredSourceTypes
      ? sampleSource.toLowerCase().indexOf(searchString) > -1
      : true;
    const isSampleSourceIncludedInSources = isAzureProject
      ? sourceTypesIncludeSampleSource(sourceTypes)
      : sourceTypesIncludeS3(sourceTypes);

    return (
      (fileStoreSources.length > 0 || renderSampleSource) && (
        <div className="SelectSourceType">
          <div className="main">
            {lakehouseSources.length > 0 && (
              <div className="source-type-section">
                <div className="source-type-header">
                  {intl.formatMessage({ id: "Source.LakehouseCatalogs" })}
                </div>
                {this.renderSourceTypes(
                  this.getEnabledSourceTypes(lakehouseSources),
                )}
                {this.renderSourceTypes(
                  this.getDisabledSourceTypes(lakehouseSources),
                )}
              </div>
            )}
            {(fileStoreSources.length > 0 || renderSampleSource) && (
              <div className="source-type-section">
                <div className="source-type-header">
                  {intl.formatMessage({ id: "Source.ObjectStorage" })}
                </div>
                {this.renderSourceTypes(
                  this.getEnabledSourceTypes(fileStoreSources),
                )}
                {isSampleSourceIncludedInSources &&
                  renderSampleSource &&
                  this.renderSampleSource()}
                {this.renderSourceTypes(
                  this.getDisabledSourceTypes(fileStoreSources),
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
        {this.renderLakehouseSources()}
        {this.renderExternalSources()}
      </>
    );
  }
}
