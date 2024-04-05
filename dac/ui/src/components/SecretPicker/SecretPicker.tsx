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
import * as React from "react";
import { Select, SelectOption } from "dremio-ui-lib/components";
import { Input } from "dremio-ui-lib/components";

type PossibleProviders = "DREMIO";

export type Provider = {
  value: PossibleProviders;
  label: string;
};

type SecretPickerValue = { provider: "DREMIO"; secret: string };

export type SecretPickerProps = {
  availableProviders: Array<Provider>;
  value?: SecretPickerValue | "" | null;
  onChange: (value: SecretPickerValue) => void;
  error?: string;
};

export const SecretPicker = ({
  availableProviders,
  value: valProp,
  onChange,
  error,
}: SecretPickerProps) => {
  const value = React.useMemo(() => {
    if (valProp === "" || !valProp) {
      return { type: "DREMIO", value: "" };
    } else {
      return valProp;
    }
  }, [valProp]) as SecretPickerValue;

  const renderProvider = (value: string | Provider) => {
    const provider =
      typeof value !== "string"
        ? value
        : availableProviders.find((provider) => provider.value === value);
    if (!provider) return "Select a provider";
    return <span>{provider.label}</span>;
  };

  const providerSelectOption = (provider: Provider) => {
    return (
      <SelectOption value={provider.value}>
        {renderProvider(provider)}
      </SelectOption>
    );
  };

  const renderLabel = (vaultType?: string | null) => {
    if (!vaultType) {
      return <></>;
    } else {
      return renderProvider(vaultType);
    }
  };

  const handleSelectChange = (newVaultType: string) => {
    onChange({ provider: newVaultType as any, secret: "" } as any);
  };

  const onInputChange = ({ target: { value: secret } }: any) => {
    onChange({
      ...value,
      secret,
    } as any);
  };

  const inputProps = {
    value: value.secret,
    onChange: onInputChange,
    ...(!!error && {
      style: {
        borderColor: "var(--dremio--color--status--error--foreground)",
        "aria-invalid": !!error,
      },
    }),
  };

  return (
    <div className="secret-picker flex">
      <div className="secret-picker__select mr-2" style={{ minWidth: "240px" }}>
        <Select
          value={value.provider}
          onChange={handleSelectChange as any}
          renderButtonLabel={renderLabel}
        >
          {availableProviders.map(providerSelectOption)}
        </Select>
      </div>
      <div className="secret-picker__input flex-1">
        <Input type="password" autoComplete="off" {...inputProps} />
      </div>
    </div>
  );
};
