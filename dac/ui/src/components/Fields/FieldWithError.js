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
import { cloneElement, Children, Component, createRef } from "react";

import PropTypes from "prop-types";

import { Tooltip } from "components/Tooltip";

import { FormValidationMessage, HoverHelp } from "dremio-ui-lib";

import forms from "uiTheme/radium/forms";
import classNames from "clsx";

class FieldWithError extends Component {
  static defaultProps = {
    errorPlacement: "right",
  };

  static propTypes = {
    label: PropTypes.string,
    touched: PropTypes.bool,
    error: PropTypes.string,
    errorPlacement: PropTypes.string, // Todo: Remove this prop and all the references to it
    style: PropTypes.object,
    labelStyle: PropTypes.object,
    labelClass: PropTypes.string,
    labelEndChildren: PropTypes.node,
    children: PropTypes.node.isRequired,
    name: PropTypes.string,
    hoverHelpText: PropTypes.string,
    hoverHelpPlacement: PropTypes.string,
    className: PropTypes.string,
    labelTooltip: PropTypes.string,
  };

  constructor(props) {
    super(props);
    this.labelTargetRef = createRef();
    this.state = {
      overLabel: false,
    };
  }

  onMouseEnterLabel = () => {
    this.setState({ overLabel: true });
  };

  onMouseLeaveLabel = () => {
    this.setState({ overLabel: false });
  };

  render() {
    const { style, children, touched, error, name, className } = this.props;

    const showError = Boolean(touched && error);

    return (
      <div
        className={classNames({
          "field-with-error field": true,
          [className]: !!className,
        })}
        data-qa={name}
        style={{ ...(style || {}), position: "relative" }}
      >
        {this.renderLabel()}
        {cloneElement(Children.only(children), { ref: "target" })}
        {showError && (
          <FormValidationMessage className="margin-top-half" dataQa="error">
            {error}
          </FormValidationMessage>
        )}
      </div>
    );
  }

  renderLabel() {
    const {
      label,
      hoverHelpText,
      hoverHelpPlacement,
      labelClass,
      labelTooltip,
      labelEndChildren,
    } = this.props;
    const { overLabel } = this.state;

    const hoverHelp = hoverHelpText ? (
      <HoverHelp placement={hoverHelpPlacement} content={hoverHelpText} />
    ) : null;

    // todo: <label> is not correctly associated with the input here (i.e. it is broken and doing nothing)
    // todo: hoverHelp should not be inside the <label>
    return (
      label && (
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <label
            className={classNames(
              {
                [labelClass]: !!labelClass,
              },
              "no-select"
            )}
            style={{
              ...forms.label,
              ...styles.label,
              ...(this.props.labelStyle || {}),
            }}
            htmlFor={this.props.name}
          >
            <div
              onMouseEnter={this.onMouseEnterLabel}
              onMouseLeave={this.onMouseLeaveLabel}
              ref={this.labelTargetRef}
            >
              {label}
            </div>
            {hoverHelp}
          </label>
          {labelEndChildren}
          <Tooltip
            container={this}
            target={() =>
              overLabel && labelTooltip ? this.labelTargetRef.current : null
            }
            placement={"top-start"}
            type="status"
            dataQa="status"
            style={{ zIndex: 40000 }}
          >
            {labelTooltip}
          </Tooltip>
        </div>
      )
    );
  }
}

const styles = {
  label: {
    display: "flex",
    alignItems: "center",
    color: "var(--color--default)",
    fontWeight: 400,
    height: "32px",
    marginBottom: "0px",
  },
};
export default FieldWithError;
