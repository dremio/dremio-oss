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
import { overlay } from "uiTheme/radium/overlay";
import Immutable from "immutable";
import { Button } from "dremio-ui-lib/components";
import FontIcon from "components/Icon/FontIcon";
import { Toggle } from "components/Fields";
import AggregateForm from "components/Aggregate/AggregateForm";
import Spinner from "components/Spinner";

import "@app/uiTheme/less/commonModifiers.less";
import "@app/uiTheme/less/Acceleration/Acceleration.less";
import { commonThemes } from "../commonThemes";

import LayoutInfo from "../LayoutInfo";
import { intl } from "@app/utils/intl";

class AccelerationAggregate extends PureComponent {
  static getFields() {
    return AggregateForm.getFields();
  }
  static validate(values) {
    return AggregateForm.validate(values);
  }
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    reflection: PropTypes.instanceOf(Immutable.Map), // option (e.g. brand new)
    location: PropTypes.object,
    fields: PropTypes.object.isRequired,
    style: PropTypes.object,
    shouldHighlight: PropTypes.bool,
    errorMessage: PropTypes.node,
    loadingRecommendations: PropTypes.bool,
    skipRecommendations: PropTypes.func,
    canAlter: PropTypes.any,
    className: PropTypes.any,
  };

  static defaultProps = {
    fields: {},
  };

  mapSchemaToColumns() {
    return this.props.dataset.get("fields").map((item, index) => {
      return Immutable.fromJS({
        type: item.getIn(["type", "name"]),
        name: item.get("name"),
        index,
      });
    });
  }

  renderForm() {
    const {
      location,
      fields,
      dataset,
      loadingRecommendations,
      skipRecommendations,
      canAlter,
    } = this.props;
    const columns = this.mapSchemaToColumns();

    if (loadingRecommendations) {
      return (
        <div
          style={overlay}
          className="AccelerationAggregate__form view-state-wrapper-overlay"
        >
          <div>
            <Spinner
              message={
                <span style={{ display: "flex", alignItems: "center" }}>
                  {intl.formatMessage({ id: "Reflections.Auto.Aggregation" })}
                  <Button
                    style={{ marginLeft: "1em" }}
                    onClick={skipRecommendations}
                    variant="secondary"
                  >
                    {intl.formatMessage({ id: "Reflections.Skip" })}
                  </Button>
                </span>
              }
            />
          </div>
        </div>
      );
    } else {
      return (
        <AggregateForm
          canAlter={canAlter}
          dataset={Immutable.fromJS({ displayFullPath: dataset.get("path") })} // fake just enough of the legacy DS model
          className={"AccelerationAggregate__AggregateForm"}
          fields={fields}
          columns={columns}
          location={location}
          canSelectMeasure={false}
          canUseFieldAsBothDimensionAndMeasure
        />
      );
    }
  }

  render() {
    const { fields, className, reflection, errorMessage } = this.props;
    const { enabled } = fields.aggregationReflections[0];

    const toggleLabel = (
      <h3 className={"AccelerationAggregate__toggleLabel"}>
        <FontIcon type="Aggregate" theme={commonThemes.rawIconTheme} />
        {laDeprecated("Aggregation Reflections")}
      </h3>
    );
    return (
      <div
        className={`AccelerationAggregate ${className}`}
        data-qa="aggregation-basic"
      >
        <div
          // DX-34369: do we need this.props.shouldHighlight ternary?
          className={`AccelerationAggregate__header
            ${this.props.shouldHighlight ? "--bgColor-highlight" : ""}`}
          data-qa="aggregation-queries-toggle"
        >
          <Toggle
            {...enabled}
            label={toggleLabel}
            className={"AccelerationAggregate__toggle"}
          />
          <LayoutInfo
            layout={reflection}
            className={"AccelerationAggregate__layout"}
          />
        </div>
        <div className={"position-relative"}>{errorMessage}</div>
        {this.renderForm()}
      </div>
    );
  }
}
export default AccelerationAggregate;
