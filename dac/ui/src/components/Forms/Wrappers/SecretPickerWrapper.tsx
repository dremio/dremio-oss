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
import { FieldWithError } from "@app/components/Fields";
import { ElementConfig } from "@app/types/Sources/SourceFormTypes";
import { type Provider, SecretPicker } from "../../SecretPicker/SecretPicker";
import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";

type SecretPickerWrapperProps = {
  elementConfig: ElementConfig;
  fields: Record<string, any>;
  field: any;
};

export const SecretPickerWrapper = ({
  elementConfig,
  field,
}: SecretPickerWrapperProps) => {
  const config = elementConfig.getConfig();
  const availableProviders: Provider[] = [{ value: "DREMIO", label: "Dremio" }];

  return (
    <FieldWithError
      errorPlacement="bottom"
      label={
        config.label || getIntlContext().t("FormControl.SecretPicker.Label")
      }
      {...field}
    >
      <SecretPicker
        isTextArea={config.supportsMultiLine}
        value={field.value}
        onChange={field.onChange}
        availableProviders={availableProviders}
      />
    </FieldWithError>
  );
};
