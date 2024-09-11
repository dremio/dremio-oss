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
import { Button } from "dremio-ui-lib/components";
import WizardFooter from "./WizardFooter";

class DefaultWizardFooter extends Component {
  static propTypes = {
    style: PropTypes.object,
    isPreviewAvailable: PropTypes.bool,
    submitting: PropTypes.bool,
    handleSubmit: PropTypes.func,
    onFormSubmit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
  };

  static defaultProps = {
    isPreviewAvailable: true,
  };

  constructor(props) {
    super(props);
    this.state = {
      submitType: null,
    };
  }

  onCancel = (e) => {
    e.preventDefault();
    this.props.onCancel();
  };

  onButtonClick = (submitType, e, ...rest) => {
    if (e && e.preventDefault) {
      e.preventDefault();
    }
    this.setState({ submitType });
    const { handleSubmit, onFormSubmit } = this.props;
    if (onFormSubmit && handleSubmit) {
      return handleSubmit((values) => {
        return onFormSubmit(values, submitType);
      })(e, ...rest);
    }
    return onFormSubmit(submitType);
  };

  render() {
    const { submitting, isPreviewAvailable } = this.props;
    const submitType = submitting && this.state.submitType;

    return (
      <WizardFooter style={{ ...styles.wizardParent, ...this.props.style }}>
        <Button
          variant="primary"
          onClick={this.onButtonClick.bind(this, "apply")}
          pending={submitType === "apply"}
        >
          Apply
        </Button>
        {isPreviewAvailable && (
          <Button
            variant="secondary"
            className="ml-1"
            onClick={this.onButtonClick.bind(this, "preview")}
            disabled={submitting && submitType !== "preview"}
            pending={submitType === "preview"}
          >
            Preview
          </Button>
        )}
        <Button className="ml-1" variant="secondary" onClick={this.onCancel}>
          Cancel
        </Button>
      </WizardFooter>
    );
  }
}

const styles = {
  wizardParent: {
    width: "100%",
  },
};
export default DefaultWizardFooter;
