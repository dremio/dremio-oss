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
import { compose } from "redux";
import { injectIntl } from "react-intl";
import { Button } from "dremio-ui-lib";
import { FormTitle } from "@app/components/Forms";
import EllipsedText from "@app/components/EllipsedText";
import * as ButtonTypes from "components/Buttons/ButtonTypes";
import { intl } from "@app/utils/intl";

function AccelerationFormMixin(input) {
  Object.assign(input.prototype, {
    // eslint-disable-line no-restricted-properties
    renderHeader() {
      const { mode } = this.state;
      const {
        intl: { formatMessage },
      } = this.props;
      const switchModeText =
        mode === "BASIC"
          ? formatMessage({ id: "Reflections.Mode.Advanced" })
          : formatMessage({ id: "Reflections.Mode.Basic" });
      const hoverTextForButton =
        mode === "BASIC"
          ? formatMessage({ id: "Reflections.Button.Advanced" })
          : formatMessage({ id: "Reflections.Button.Basic" });
      return (
        <div>
          <div style={{ float: "right", display: "flex", marginTop: "5px" }}>
            {mode === "ADVANCED" && (
              <Button
                disableMargin
                onClick={this.clearReflections}
                type={ButtonTypes.SECONDARY}
                text={intl.formatMessage({ id: "Reflections.Remove.All" })}
                style={{ fontSize: 10 }}
              />
            )}
            <EllipsedText text={hoverTextForButton}>
              <Button
                disabled={mode === "ADVANCED" && this.getMustBeInAdvancedMode()}
                disableMargin
                onClick={this.toggleMode}
                color={ButtonTypes.UI_LIB_SECONDARY}
                style={{ fontSize: 10, marginLeft: 10 }}
                text={switchModeText}
              />
            </EllipsedText>
          </div>
          <FormTitle>{la("Reflections")}</FormTitle>
        </div>
      );
    },
  });
  return input;
}

export const AccelerationFormWithMixin = (accelerationForm) => {
  return compose(injectIntl, AccelerationFormMixin)(accelerationForm);
};
