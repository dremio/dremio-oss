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
import { shallow } from "enzyme";

import { sourceProperties } from "#oss/constants/sourceTypes";
import { expect } from "chai";
import { addSourcePresetTiles } from "#oss/utils/sourceUtils";
import SelectSourceType from "./SelectSourceType";

describe("SelectSourceType", () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      onSelectSource: sinon.spy(),
      onAddSampleSource: sinon.spy(),
      sourceTypes: sourceProperties,
    };
    commonProps = {
      ...minimalProps,
    };
  });

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<SelectSourceType {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });

  describe("Iceberg REST catalog presets", () => {
    const sourceTypes = addSourcePresetTiles([
      { sourceType: "S3", label: "Amazon S3" },
      { sourceType: "RESTCATALOG", label: "Iceberg REST Catalog" },
      { sourceType: "MYSQL", label: "MySQL" },
    ]);
    const findTile = (wrapper, label) =>
      wrapper
        .find("SelectConnectionButton")
        .filterWhere((button) => button.prop("label") === label);

    it("renders the generic and the Polaris tile as lakehouse catalogs", () => {
      const wrapper = shallow(
        <SelectSourceType {...commonProps} sourceTypes={sourceTypes} />,
      );
      const generic = findTile(wrapper, "Iceberg REST Catalog");
      const polaris = findTile(wrapper, "Apache Polaris OSS");
      expect(generic).to.have.length(1);
      expect(polaris).to.have.length(1);
      // same icon as the base type, but a distinct React key
      expect(polaris.prop("dremioIcon")).to.equal("sources/RESTCATALOG");
      expect(polaris.key()).to.equal("RESTCATALOG:POLARIS");
      expect(generic.key()).to.equal("RESTCATALOG");
      const lakehouseSection = wrapper.find(".source-type-section").first();
      expect(
        lakehouseSection
          .find("SelectConnectionButton")
          .map((button) => button.prop("label")),
      ).to.deep.equal(["Apache Polaris OSS", "Iceberg REST Catalog"]);
    });

    it("passes the preset id of the clicked tile", () => {
      const wrapper = shallow(
        <SelectSourceType {...commonProps} sourceTypes={sourceTypes} />,
      );
      findTile(wrapper, "Apache Polaris OSS").prop("onClick")();
      expect(commonProps.onSelectSource).to.have.been.calledOnce;
      expect(commonProps.onSelectSource.firstCall.args[0]).to.include({
        sourceType: "RESTCATALOG",
        presetId: "POLARIS",
      });
      findTile(wrapper, "Iceberg REST Catalog").prop("onClick")();
      expect(commonProps.onSelectSource.secondCall.args[0].presetId).to.be
        .undefined;
    });

    it("finds the Polaris tile by search", () => {
      const wrapper = shallow(
        <SelectSourceType {...commonProps} sourceTypes={sourceTypes} />,
      );
      wrapper.instance().updateSourcesList(
        sourceTypes.filter((type) =>
          type.label.toLowerCase().includes("polaris"),
        ),
        "polaris",
      );
      expect(
        wrapper
          .find("SelectConnectionButton")
          .map((button) => button.prop("label")),
      ).to.deep.equal(["Apache Polaris OSS"]);
    });
  });
});
