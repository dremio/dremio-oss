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

import { applyTheme } from "#oss/theme";
import { AppearancePicker } from "dremio-ui-common/components/AppearancePicker/AppearancePicker.js";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";

export default () => {
  const { t } = getIntlContext();
  return (
    <div className="">
      <div className="settingHeader__root">
        <div className="settingHeader__title px-2">Appearance</div>
      </div>

      <div className="p-1 px-2 admin-preferences-settings-main">
        <div className="preferences-settings-page-description">
          {t("AccountSettings.AppearancePicker.Common.Description")}
        </div>
        <div className="mt-3">
          <AppearancePicker
            onThemeChanged={() => {
              applyTheme();
            }}
          />
        </div>
      </div>
    </div>
  );
};
