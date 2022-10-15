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

import { Button } from "dremio-ui-lib";
import * as ButtonTypes from "components/Buttons/ButtonTypes";
import WizardFooter from "./WizardFooter";
import { intl } from "@app/utils/intl";

class StepWizard extends PureComponent {
  static propTypes = {
    changeFormType: PropTypes.func.isRequired,
    onCancelClick: PropTypes.func.isRequired,
    onNextClick: PropTypes.func,
    style: PropTypes.object,
    hasActiveDataset: PropTypes.bool,
  };

  render() {
    return (
      <WizardFooter style={this.props.style}>
        <Button
          disabled={!this.props.hasActiveDataset}
          onMouseDown={this.props.changeFormType.bind(this, "apply")}
          onClick={this.props.onNextClick}
          color={ButtonTypes.UI_LIB_PRIMARY}
          style={{ marginBottom: 0 }}
          disableMargin
          key="details-wizard-next"
          text={intl.formatMessage({ id: "Common.Next" })}
        />
        <Button
          style={{ marginLeft: 5, marginBottom: 0, marginRight: 5 }}
          color={ButtonTypes.UI_LIB_SECONDARY}
          disableMargin
          onClick={this.props.onCancelClick}
          key="details-wizard-cancel"
          text={intl.formatMessage({ id: "Common.Cancel" })}
        />
      </WizardFooter>
    );
  }
}
export default StepWizard;
