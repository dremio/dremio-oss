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
import PropTypes from "prop-types";
import Immutable from "immutable";

import FontIcon from "components/Icon/FontIcon";
import SimpleButton from "components/Buttons/SimpleButton";
import { createReflectionFormValues } from "utils/accelerationUtils";
import AccelerationAggregationMixin from "@inject/components/Acceleration/Advanced/AccelerationAggregationMixin.js";

import "@app/uiTheme/less/Acceleration/Acceleration.less";
import * as classes from "@app/uiTheme/radium/replacingRadiumPseudoClasses.module.less";
import { commonThemes } from "../commonThemes";
import AccelerationGridController from "./AccelerationGridController";
import clsx from "clsx";

@AccelerationAggregationMixin
export default class AccelerationAggregation extends Component {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    reflections: PropTypes.instanceOf(Immutable.Map).isRequired,
    fields: PropTypes.object,
    canAlter: PropTypes.any,
    allowPartitionTransform: PropTypes.bool,
    aggregationRecommendation: PropTypes.object,
    loadingRecommendations: PropTypes.bool,
  };

  static getFields() {
    return [
      "aggregationReflections[].id",
      "aggregationReflections[].tag",
      "aggregationReflections[].type",
      "aggregationReflections[].name",
      "aggregationReflections[].enabled",
      "aggregationReflections[].partitionDistributionStrategy",
      "aggregationReflections[].arrowCachingEnabled",
      "aggregationReflections[].partitionFields[].name",
      "aggregationReflections[].sortFields[].name",
      "aggregationReflections[].dimensionFields[].name",
      "aggregationReflections[].dimensionFields[].granularity",
      "aggregationReflections[].measureFields[].name",
      "aggregationReflections[].measureFields[].measureTypeList",
      "aggregationReflections[].distributionFields[].name",
      "aggregationReflections[].shouldDelete",
    ];
  }

  static validate() {
    return {};
  }

  addNewLayout = () => {
    const { allowPartitionTransform, aggregationRecommendation } = this.props;
    const { aggregationReflections } = this.props.fields;

    const reflection = createReflectionFormValues(
      allowPartitionTransform && aggregationRecommendation
        ? aggregationRecommendation
        : {
            type: "AGGREGATION",
          },
      aggregationReflections.map((e) => e.name.value)
    );

    aggregationReflections.addField(reflection);
  };

  renderHeader = () => {
    return (
      <div className={"AccelerationAggregation__header"}>
        <h3 className={"AccelerationAggregation__toggleLabel"}>
          <FontIcon
            type="Aggregate"
            theme={commonThemes.aggregationIconTheme}
          />
          {laDeprecated("Aggregation Reflections")}
        </h3>
        <SimpleButton
          onClick={this.addNewLayout}
          buttonStyle="secondary"
          className={clsx(classes["secondaryButtonPsuedoClasses"])}
          // DX-34369
          style={
            this.checkIfButtonShouldBeRendered()
              ? { minWidth: "110px" }
              : { display: "none" }
          }
          type="button"
        >
          {laDeprecated("New Reflection")}
        </SimpleButton>
      </div>
    );
  };

  render() {
    const {
      dataset,
      reflections,
      fields: { aggregationReflections },
      canAlter,
      loadingRecommendations,
    } = this.props;
    return (
      <div className={"AccelerationAggregation"}>
        {this.renderHeader()}
        <AccelerationGridController
          canAlter={canAlter}
          dataset={dataset}
          reflections={reflections}
          layoutFields={aggregationReflections}
          loadingRecommendations={loadingRecommendations}
          activeTab="aggregation"
        />
      </div>
    );
  }
}
