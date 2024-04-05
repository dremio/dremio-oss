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
import React from "react";
import { shallow } from "enzyme";

import Select from "../Select";

import FormikSelect from "./FormikSelect";

const mockOnChange = jest.fn();

const defaultProps = {
  field: {
    name: "sample-select-name",
    onChange: mockOnChange,
  },
  options: [
    { label: "label 1", value: "val1" },
    { label: "label 2", value: "val2" },
  ],
};

const getShallowWrapper = (props = defaultProps) => {
  return shallow(<FormikSelect {...props} />);
};

describe("Formik Select", () => {
  const wrapper = getShallowWrapper();
  it("has the required components with props", () => {
    expect(wrapper.find(Select).exists()).toBe(true);
    expect(wrapper.find(Select).props()).toEqual(
      expect.objectContaining({
        ...defaultProps.field,
        options: defaultProps.options,
      }),
    );
  });
});
