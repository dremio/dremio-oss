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

import { Meta, StoryFn } from "@storybook/react";

import {
  SectionMessage,
  Select,
  SelectOption,
  SelectGroup,
  useSelect,
} from "../../../components";

export default {
  title: "Input Controls/Select",
  component: Select,
} as Meta<typeof Select>;

const regions = [
  {
    id: "us-west-1",
    title: "US West (N. California)",
    provider: "AWS",
  },
  {
    id: "us-west-2",
    title: "US West (Oregon)",
    provider: "AWS",
  },
  {
    id: "us-east-1",
    title: "US East (N. Virginia)",
    provider: "AWS",
    disabled: true,
  },
  {
    id: "us-east-2",
    title: "US East (Ohio)",
    provider: "AWS",
  },
  {
    id: "ca-central-1",
    title: "Canada (Central)",
    provider: "AWS",
  },
  {
    id: "east-us",
    title: "East US",
    provider: "Azure",
  },
  {
    id: "east-us-2",
    title: "East US 2",
    provider: "Azure",
  },
] as const;

const regionToLabel = (region: typeof regions[number]) => (
  <span>
    {region.title} <small>{region.id}</small>
  </span>
);

const regionByProvider =
  (provider: typeof regions[number]["provider"]) =>
  (region: typeof regions[number]): boolean => {
    return region.provider === provider;
  };

const regionToOption = (region: typeof regions[number]) => (
  <SelectOption key={region.id} value={region.id} disabled={!!region.disabled}>
    {regionToLabel(region)}
  </SelectOption>
);

const renderRegionButtonLabel = (regionId: typeof regions[number]["id"]) => {
  if (!regionId) {
    return "Select a region";
  }
  return regionToLabel(regions.find((region) => region.id === regionId));
};

export const Basic: StoryFn<typeof Select> = () => {
  const regionSelection = useSelect();

  return (
    <div style={{ fontSize: "14px" }} className="dremio-prose">
      <div className="form-group">
        <label htmlFor="region">Region</label>
        <Select
          {...regionSelection}
          id="region"
          renderButtonLabel={renderRegionButtonLabel}
        >
          <SelectGroup label="AWS">
            {regions.filter(regionByProvider("AWS")).map(regionToOption)}
          </SelectGroup>
          <SelectGroup label="Azure">
            {regions.filter(regionByProvider("Azure")).map(regionToOption)}
          </SelectGroup>
        </Select>
      </div>
    </div>
  );
};

const catalogs = [
  { id: "firstRepo" },
  { id: "test" },
  { id: "polar" },
  { id: "some_other_cloud", disabled: true },
  { id: "another_cloud", disabled: true },
] as const;

const renderCatalogLabel = (catalog: typeof catalogs[number]) => (
  <div className="icon-label">
    <dremio-icon name="brand/arctic-catalog-source"></dremio-icon> {catalog.id}
  </div>
);

const catalogToOption = (catalog: typeof catalogs[number]) => (
  <SelectOption
    key={catalog.id}
    value={catalog.id}
    disabled={!!catalog.disabled}
  >
    {renderCatalogLabel(catalog)}
  </SelectOption>
);

const renderButtonLabel = (catalogId: typeof catalogs[number]["id"]) => {
  if (!catalogId) {
    return "Select a catalog";
  }
  return renderCatalogLabel(
    catalogs.find((catalog) => catalog.id === catalogId)
  );
};

export const Grouped: StoryFn<typeof Select> = () => {
  const catalogSelection = useSelect();
  const handleChange = (value: string) => {
    if (value === "__ADD_BUTTON__") {
      prompt("Enter new Arctic catalog name");
      return;
    }
    catalogSelection.onChange(value);
  };
  return (
    <div style={{ fontSize: "14px" }} className="dremio-prose">
      <div className="form-group">
        <label htmlFor="catalog">Choose a catalog</label>
        <Select
          {...catalogSelection}
          id="catalog"
          onChange={handleChange}
          renderButtonLabel={renderButtonLabel}
        >
          <SelectGroup>
            <SelectOption value="__ADD_BUTTON__">
              <div className="icon-label">
                <dremio-icon name="interface/add" alt=""></dremio-icon> Add a
                catalog
              </div>
            </SelectOption>
          </SelectGroup>
          <SelectGroup>
            {catalogs.filter((option) => !option.disabled).map(catalogToOption)}
          </SelectGroup>
          <SelectGroup>
            <li role="presentation">
              <SectionMessage appearance="information" className="m-1">
                These catalogs have already been added to the project.
              </SectionMessage>
            </li>
            {catalogs
              .filter((option) => !!option.disabled)
              .map(catalogToOption)}
          </SelectGroup>
        </Select>
      </div>
    </div>
  );
};
