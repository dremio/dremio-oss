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

import ApiUtils from "utils/apiUtils/apiUtils";
import SourceFormJsonPolicy from "utils/FormUtils/SourceFormJsonPolicy";
import ConfigurableSourceForm from "pages/HomePage/components/modals/ConfigurableSourceForm";

import { getResponseForEntity } from "testUtil";

import { NAS } from "dyn-load/constants/sourceTypes";

import { AddSourceModal } from "./AddSourceModal";

describe.skip("AddSourceModal", () => {
  const selectedSource = { label: "NAS", sourceType: NAS };

  let minimalProps;
  let commonProps;
  let context;
  beforeEach(() => {
    minimalProps = {
      location: {
        pathname: "somePathname",
        state: {},
      },
      hide: sinon.spy(),
      showConfirmationDialog: sinon.spy(),
      updateFormDirtyState: sinon.spy(),
      createSource: sinon.stub().returns(
        Promise.resolve(
          getResponseForEntity("source", "someId", {
            id: "someId",
            links: {
              self: "someUrl",
            },
          }),
        ),
      ),
      spaces: Immutable.fromJS([{}]),
      sources: Immutable.fromJS([{}]),
      createSampleSource: sinon.stub().resolves({
        payload: Immutable.fromJS({
          entities: {
            source: { "new-id": { id: "new-id", links: { self: "/self" } } },
          },
          result: "new-id",
        }),
      }),
    };
    commonProps = {
      ...minimalProps,
      isOpen: true,
    };
    context = {
      router: {
        push: sinon.spy(),
      },
    };
  });

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<AddSourceModal {...minimalProps} />, { context });
    expect(wrapper).to.have.length(1);
  });

  it("should render SelectSourceType when no source type selected", () => {
    const wrapper = shallow(<AddSourceModal {...commonProps} />, { context });
    expect(wrapper.find("SelectSourceType")).to.have.length(1);
  });

  describe("#handleAddSampleSource()", () => {
    it("should navigate to source on success", () => {
      const instance = shallow(<AddSourceModal {...commonProps} />, {
        context,
      }).instance();
      const promise = instance.handleAddSampleSource();
      expect(instance.state.isAddingSampleSource).to.be.true;
      return promise.then(() => {
        expect(context.router.push).to.have.been.calledWith("/self");
        expect(instance.state.isAddingSampleSource).to.be.false;
        expect(commonProps.hide).to.have.been.called;
        return null;
      });
    });
    it("should reset state.isAddingSampleSource on http error", () => {
      commonProps.createSampleSource = sinon.stub().resolves({ error: true });
      const instance = shallow(<AddSourceModal {...commonProps} />, {
        context,
      }).instance();
      const promise = instance.handleAddSampleSource();
      expect(instance.state.isAddingSampleSource).to.be.true;
      return promise.then(() => {
        expect(context.router.push).to.have.not.been.called;
        expect(instance.state.isAddingSampleSource).to.be.false;
        expect(commonProps.hide).to.have.been.called;
        return null;
      });
    });
  });

  describe("handleSelectSource", () => {
    it("should call setStateWithSourceTypeConfigFromServer", () => {
      const wrapper = shallow(<AddSourceModal {...commonProps} />, { context });
      const instance = wrapper.instance();
      sinon.spy(instance, "setStateWithSourceTypeConfigFromServer");
      wrapper.instance().handleSelectSource(selectedSource);
      expect(instance.setStateWithSourceTypeConfigFromServer).to.be.called;
    });
  });

  describe("submit", () => {
    let instance;
    let formValues;
    beforeEach(() => {
      sinon.spy(ApiUtils, "attachFormSubmitHandlers");
      formValues = {
        name: "someName",
        metadataPolicy: {
          namesRefreshMillis: {},
          datasetDefinitionRefreshAfterMillis: {},
          datasetDefinitionExpireAfterMillis: {},
          authTTLMillis: {},
        },
      };
      const wrapper = shallow(
        <AddSourceModal {...commonProps} source={selectedSource} />,
        { context },
      );
      instance = wrapper.instance();
    });
    afterEach(() => {
      ApiUtils.attachFormSubmitHandlers.restore();
    });
    it("should call mutateFormValues and createSource", () => {
      sinon.spy(instance, "mutateFormValues");
      instance.handleAddSourceSubmit(formValues);
      expect(instance.mutateFormValues).to.be.calledOnce;
      expect(ApiUtils.attachFormSubmitHandlers).to.be.calledOnce;
      expect(commonProps.createSource).to.be.calledOnce;
    });

    it("should call router.push with new source's url", () => {
      return instance.handleAddSourceSubmit(formValues).then(() => {
        expect(context.router.push).to.be.calledWith("someUrl");
        return null;
      });
    });
  });

  describe("#startTrackSubmitTime", () => {
    beforeEach(function () {
      this.clock = sinon.useFakeTimers();
    });
    afterEach(function () {
      this.clock.restore();
    });

    it("isSubmitTakingLong should be true after 5 seconds", function () {
      const instance = shallow(
        <AddSourceModal {...commonProps} source={selectedSource} />,
        { context },
      ).instance();
      instance.startTrackSubmitTime();
      this.clock.tick(5000);
      expect(instance.state.isSubmitTakingLong).to.be.true;
      expect(instance.state.submitTimer).to.not.be.null;
    });
  });

  describe("#stopTrackSubmitTime", () => {
    it("isSubmitTakingLong should be false and reset submitTimer", () => {
      const instance = shallow(
        <AddSourceModal {...commonProps} source={selectedSource} />,
        { context },
      ).instance();
      instance.setState({
        isSubmitTakingLong: true,
      });
      instance.stopTrackSubmitTime();
      expect(instance.state.isSubmitTakingLong).to.be.false;
      expect(instance.state.submitTimer).to.be.null;
    });
  });
});

describe("AddSourceModal (Iceberg REST catalog)", () => {
  // minimal stand-in for the FormConfig built by SourceFormJsonPolicy
  const restCatalogConfig = {
    sourceType: "RESTCATALOG",
    label: "Iceberg REST Catalog",
    form: {
      getFields: () => [],
      addValidators: (accumulator) => accumulator,
    },
  };
  let props;
  let context;
  let fetchJson;
  let getCombinedConfig;

  beforeEach(() => {
    // tests call the fetchJson handlers themselves
    fetchJson = sinon.stub(ApiUtils, "fetchJson").resolves();
    getCombinedConfig = sinon.stub(SourceFormJsonPolicy, "getCombinedConfig");
    props = {
      location: { pathname: "somePathname", state: {} },
      isOpen: true,
      hide: sinon.spy(),
      updateFormDirtyState: sinon.spy(),
      createSource: sinon.stub(),
      createSampleSource: sinon.stub(),
      createSampleDbSource: sinon.stub(),
      dispatchPassDataBetweenTabs: sinon.spy(),
      loadGrant: sinon.spy(),
    };
    context = { router: { push: sinon.spy() } };
  });

  afterEach(() => {
    fetchJson.restore();
    getCombinedConfig.restore();
  });

  const render = () => shallow(<AddSourceModal {...props} />, { context });

  it("adds the Apache Polaris preset tile to the loaded source types", () => {
    const instance = render().instance();
    fetchJson.resetHistory();
    instance.setStateWithSourceTypeListFromServer();
    const [endpoint, onSuccess] = fetchJson.firstCall.args;
    expect(endpoint).to.equal("source/type");
    onSuccess({
      data: [
        { sourceType: "RESTCATALOG", label: "Iceberg REST Catalog" },
        { sourceType: "S3", label: "Amazon S3" },
      ],
    });
    expect(
      instance.state.sourceTypes.map(({ label, presetId }) => ({
        label,
        presetId,
      })),
    ).to.deep.include.members([
      { label: "Iceberg REST Catalog", presetId: undefined },
      { label: "Apache Polaris OSS", presetId: "POLARIS" },
    ]);
  });

  describe("handleSelectSource", () => {
    it("opens the RESTCATALOG form with the Polaris initial values", () => {
      const instance = render().instance();
      sinon.stub(instance, "setStateWithSourceTypeConfigFromServer");
      instance.handleSelectSource({
        sourceType: "RESTCATALOG",
        label: "Apache Polaris OSS",
        presetId: "POLARIS",
      });
      expect(
        instance.setStateWithSourceTypeConfigFromServer,
      ).to.have.been.calledWith("RESTCATALOG");
      expect(instance.state.selectedPresetId).to.equal("POLARIS");
      const initialValues = instance.getInitialValues();
      expect(initialValues.config.isUsingVendedCredentials).to.equal(false);
      expect(
        initialValues.config.propertyList.map(({ name, value }) => ({
          name,
          value,
        })),
      ).to.deep.equal([
        { name: "warehouse", value: "" },
        { name: "scope", value: "PRINCIPAL_ROLE:ALL" },
      ]);
      // stable between renders so that redux-form does not re-initialize the form
      expect(instance.getInitialValues()).to.deep.equal(initialValues);
    });

    it("merges preset values into initialFormValues", () => {
      props.initialFormValues = { name: "polaris", config: { foo: "bar" } };
      const instance = render().instance();
      sinon.stub(instance, "setStateWithSourceTypeConfigFromServer");
      instance.handleSelectSource({
        sourceType: "RESTCATALOG",
        presetId: "POLARIS",
      });
      const initialValues = instance.getInitialValues();
      expect(initialValues.name).to.equal("polaris");
      expect(initialValues.config.foo).to.equal("bar");
      expect(initialValues.config.propertyList).to.have.length(2);
      expect(props.initialFormValues.config.propertyList).to.be.undefined;
    });

    it("does not pre-fill the generic tile and clears a previous preset", () => {
      const instance = render().instance();
      sinon.stub(instance, "setStateWithSourceTypeConfigFromServer");
      instance.handleSelectSource({
        sourceType: "RESTCATALOG",
        presetId: "POLARIS",
      });
      instance.handleSelectSource({ sourceType: "RESTCATALOG" });
      expect(instance.state.selectedPresetId).to.be.null;
      expect(instance.getInitialValues()).to.be.undefined;
    });
  });

  it("titles the form after the preset and submits the RESTCATALOG type", async () => {
    const instance = render().instance();
    sinon.stub(instance, "setStateWithSourceTypeConfigFromServer");
    instance.handleSelectSource({
      sourceType: "RESTCATALOG",
      presetId: "POLARIS",
    });
    instance.setState({
      isTypeSelected: true,
      selectedFormType: restCatalogConfig,
    });
    expect(instance.getTitle()).to.contain("Apache Polaris OSS");

    props.createSource.resolves({ error: true });
    sinon.stub(ApiUtils, "attachFormSubmitHandlers").callsFake((p) => p);
    try {
      await instance.handleAddSourceSubmit({ name: "polaris", config: {} });
    } finally {
      ApiUtils.attachFormSubmitHandlers.restore();
    }
    expect(props.createSource).to.have.been.calledWith(
      { name: "polaris", config: {} },
      "RESTCATALOG",
    );
  });

  describe("setStateWithSourceTypeConfigFromServer", () => {
    it("does not fail when the layout has no metadataRefresh", async () => {
      getCombinedConfig.returns({ ...restCatalogConfig });
      const instance = render().instance();
      fetchJson.resetHistory();
      instance.setStateWithSourceTypeConfigFromServer("RESTCATALOG");
      const [endpoint, onSuccess] = fetchJson.firstCall.args;
      expect(endpoint).to.equal("source/type/RESTCATALOG");
      await onSuccess({ sourceType: "RESTCATALOG", elements: [] });
      expect(instance.state.isTypeSelected).to.be.true;
      expect(props.dispatchPassDataBetweenTabs).to.have.been.calledWithMatch({
        isFileSystemSource: undefined,
        isMetaStore: true,
        sourceType: "RESTCATALOG",
      });
    });

    it("shows the server error message", async () => {
      const instance = render().instance();
      fetchJson.resetHistory();
      instance.setStateWithSourceTypeConfigFromServer("RESTCATALOG");
      const onError = fetchJson.firstCall.args[2];
      await onError({
        json: () => Promise.resolve({ errorMessage: "Unknown type" }),
      });
      expect(instance.state.didSourceTypeLoadFail).to.be.true;
      expect(instance.state.errorMessage).to.equal("Unknown type");
    });

    it("handles errors that are not an HTTP response", async () => {
      const instance = render().instance();
      fetchJson.resetHistory();
      instance.setStateWithSourceTypeConfigFromServer("RESTCATALOG");
      const onError = fetchJson.firstCall.args[2];
      await onError(new TypeError("Cannot read properties of undefined"));
      expect(instance.state.didSourceTypeLoadFail).to.be.true;
      expect(instance.state.errorMessage).to.equal(
        "Failed to load source list.",
      );
    });
  });

  it("validates the property lists of RESTCATALOG sources", () => {
    const wrapper = render();
    wrapper.setState({
      isTypeSelected: true,
      selectedFormType: restCatalogConfig,
    });
    const validate = wrapper.find(ConfigurableSourceForm).prop("validate");
    expect(
      validate({
        config: { propertyList: [{ name: "credential", value: "x" }] },
      }).config.propertyList[0].name,
    ).to.contain("Catalog Credentials");
  });
});
