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
import { Button } from "dremio-ui-lib";
import * as ButtonTypes from "@app/components/Buttons/ButtonTypes";
import EngineStatus from "@app/pages/AdminPage/subpages/Provisioning/components/EngineStatus";
import { intl } from "@app/utils/intl";

export default function (input) {
  Object.assign(input.prototype, {
    // eslint-disable-line no-restricted-properties
    loadData() {},
    getEngineStatus(engine, styles) {
      return <EngineStatus engine={engine} style={styles.statusIcon} />;
    },

    renderButtons(styles, isReadOnly) {
      return (
        <Button
          style={{
            width: 110,
            marginTop: 5,
            marginLeft: 10,
          }}
          onClick={this.onEdit}
          disabled={isReadOnly}
          color={ButtonTypes.UI_LIB_PRIMARY}
          disableMargin
          text={intl.formatMessage({ id: "Common.Edit.Settings" })}
        />
      );
    },
  });
}
