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
import { Button } from "dremio-ui-lib/components";
import { FormTitle } from "#oss/components/Forms";
import EllipsedText from "#oss/components/EllipsedText";
import { intl } from "#oss/utils/intl";

function AccelerationFormMixin(input) {
  Object.assign(input.prototype, {
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
              <Button onClick={this.clearReflections} variant="secondary">
                {intl.formatMessage({ id: "Reflections.Remove.All" })}
              </Button>
            )}
            <EllipsedText text={hoverTextForButton}>
              <Button
                disabled={mode === "ADVANCED" && this.getMustBeInAdvancedMode()}
                onClick={this.toggleMode}
                variant="secondary"
                style={{ marginLeft: 10 }}
              >
                {switchModeText}
              </Button>
            </EllipsedText>
          </div>
          <FormTitle>{laDeprecated("Reflections")}</FormTitle>
        </div>
      );
    },
  });
  return input;
}

export const AccelerationFormWithMixin = (accelerationForm) => {
  return compose(injectIntl, AccelerationFormMixin)(accelerationForm);
};
