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
import Immutable from "immutable";

import ApiUtils from "utils/apiUtils/apiUtils";
import ViewStateWrapper from "components/ViewStateWrapper";
import SourceFormJsonPolicy from "utils/FormUtils/SourceFormJsonPolicy";
import ConfigurableSourceForm from "pages/HomePage/components/modals/ConfigurableSourceForm";
import { EditSourceView } from "./EditSourceView";

describe("EditSourceView", () => {
  let minimalProps;

  const contextTypes = {
    location: {},
    router: {},
  };

  beforeEach(() => {
    minimalProps = {
      sourceName: "name",
      sourceType: "type",
      hide: sinon.spy(),
      createSource: sinon.spy(),
      removeSource: sinon.spy(),
      source: new Immutable.Map({ accessControlList: new Immutable.Map() }),
      loadSource: sinon.spy(),
    };
  });

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<EditSourceView {...minimalProps} />, {
      context: contextTypes,
    });
    expect(wrapper).to.have.length(1);
  });

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<EditSourceView {...minimalProps} />, {
      context: contextTypes,
    });
    const instance = wrapper.instance();

    instance.checkIsMetadataImpacting = sinon.stub().returns(Promise.resolve());
    instance.reallySubmitEdit = () => {
      return Promise.resolve({});
    };

    return instance.submitEdit({ config: {} }).then(() => {
      expect(instance.checkIsMetadataImpacting).to.have.been.called;
      return null;
    });
  });

  describe("RESTCATALOG", () => {
    let fetchJson;
    let getCombinedConfig;
    const restCatalogConfig = {
      sourceType: "RESTCATALOG",
      label: "Iceberg REST Catalog",
      form: {
        getFields: () => [],
        addValidators: (accumulator) => accumulator,
        getTabs: () => [],
      },
    };

    beforeEach(() => {
      fetchJson = sinon.stub(ApiUtils, "fetchJson").resolves();
      getCombinedConfig = sinon
        .stub(SourceFormJsonPolicy, "getCombinedConfig")
        .returns(restCatalogConfig);
      minimalProps = {
        ...minimalProps,
        sourceType: "RESTCATALOG",
        dispatchPassDataBetweenTabs: sinon.spy(),
      };
    });

    afterEach(() => {
      fetchJson.restore();
      getCombinedConfig.restore();
    });

    it("does not fail when the layout has no metadataRefresh", async () => {
      const instance = shallow(<EditSourceView {...minimalProps} />, {
        context: contextTypes,
      }).instance();
      instance.setStateWithSourceTypeConfigFromServer("RESTCATALOG");
      const [endpoint, onSuccess] = fetchJson.lastCall.args;
      expect(endpoint).to.equal("source/type/RESTCATALOG");
      await onSuccess({ sourceType: "RESTCATALOG", elements: [] });
      expect(instance.state.isConfigLoaded).to.be.true;
      expect(
        minimalProps.dispatchPassDataBetweenTabs,
      ).to.have.been.calledWithMatch({
        isFileSystemSource: undefined,
        isMetaStore: true,
        sourceType: "RESTCATALOG",
      });
    });

    it("renders the load error instead of throwing", () => {
      const wrapper = shallow(<EditSourceView {...minimalProps} />, {
        context: contextTypes,
      });
      wrapper.setState({ didLoadFail: true });
      // the root element is the ViewStateWrapper
      expect(wrapper.type()).to.equal(ViewStateWrapper);
      expect(wrapper.prop("viewState").getIn(["error", "message"])).to.equal(
        "Failed to load source configuration.",
      );
    });

    it("validates the property lists", () => {
      const wrapper = shallow(<EditSourceView {...minimalProps} />, {
        context: contextTypes,
      });
      wrapper.setState({
        isConfigLoaded: true,
        selectedFormType: restCatalogConfig,
      });
      const validate = wrapper.find(ConfigurableSourceForm).prop("validate");
      const errors = validate({
        config: {
          propertyList: [{ name: "fs.s3a.secret.key", value: "x" }],
          secretPropertyList: [
            { name: "credential", value: "$DREMIO_EXISTING_VALUE$" },
          ],
        },
      });
      expect(errors.config.propertyList[0].name).to.contain(
        "Catalog Credentials",
      );
      expect(errors.config.secretPropertyList).to.be.undefined;
    });

    it("does not block sensitive keys the saved source already has", () => {
      const wrapper = shallow(
        <EditSourceView
          {...minimalProps}
          source={Immutable.fromJS({
            name: "polaris",
            type: "RESTCATALOG",
            config: {
              propertyList: [{ name: "credential", value: "id:secret" }],
            },
          })}
        />,
        { context: contextTypes },
      );
      wrapper.setState({
        isConfigLoaded: true,
        selectedFormType: restCatalogConfig,
      });
      const validate = wrapper.find(ConfigurableSourceForm).prop("validate");
      expect(
        validate({
          config: {
            propertyList: [{ name: "credential", value: "id:secret" }],
          },
        }),
      ).to.deep.equal({});
      const errors = validate({
        config: {
          propertyList: [
            { name: "credential", value: "id:secret" },
            { name: "token", value: "t" },
          ],
        },
      });
      expect(errors.config.propertyList[0]).to.deep.equal({});
      expect(errors.config.propertyList[1].name).to.contain(
        "Catalog Credentials",
      );
    });
  });
});
