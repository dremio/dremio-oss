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
import { useSupportFlag } from "@app/exports/endpoints/SupportFlags/getSupportFlag";
import { DATAPLANE_OAUTH_CLIENT_AUTH_ENABLED } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";
import ContainerSelection from "components/Forms/ContainerSelection";

export default function OauthWrapper({
  disabled,
  elementConfig,
  fields,
}: {
  disabled: boolean;
  elementConfig: Record<string, any>;
  fields: Record<string, any>;
}) {
  const [enabled, loading] = useSupportFlag(
    DATAPLANE_OAUTH_CLIENT_AUTH_ENABLED,
  );

  if (!loading && !enabled) {
    for (let i = 0; i < elementConfig._config.options.length; i++) {
      if (elementConfig._config.options[i].value === "OAUTH2") {
        elementConfig._config.options.splice(i, i);
      }
    }
  }

  return (
    <ContainerSelection
      disabled={disabled}
      fields={fields}
      elementConfig={elementConfig}
      hidden={loading}
    />
  );
}
