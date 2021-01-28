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
package com.dremio.dac.daemon;

import java.util.function.Supplier;

import javax.inject.Inject;
import javax.ws.rs.core.SecurityContext;

import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.internal.inject.ClassBinding;
import org.glassfish.jersey.process.internal.RequestScoped;

import com.dremio.dac.explore.QueryExecutor;
import com.dremio.dac.explore.join.JobsBasedRecommender;
import com.dremio.dac.explore.join.JoinRecommender;
import com.dremio.dac.model.sources.FormatTools;
import com.dremio.dac.server.DACSecurityContext;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.server.tokens.TokenInfo;
import com.dremio.dac.service.catalog.CatalogServiceHelper;
import com.dremio.dac.service.collaboration.CollaborationHelper;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.reflection.ReflectionServiceHelper;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DatasetCatalog;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.SourceCatalog;
import com.dremio.exec.ops.ReflectionContext;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.options.OptionManager;
import com.dremio.service.BinderImpl;
import com.dremio.service.BinderImpl.Binding;
import com.dremio.service.BinderImpl.InstanceBinding;
import com.dremio.service.BinderImpl.SingletonBinding;
import com.dremio.service.SingletonRegistry;
import com.dremio.service.reflection.ReflectionAdministrationService;

/**
 * Class to bind resources to HK2 injector.
 */
public class DremioBinder extends AbstractBinder {

  private SingletonRegistry bindings;

  public DremioBinder(SingletonRegistry bindings) {
    this.bindings = bindings;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  protected void configure() {
    for(Binding b : bindings){
      switch(b.getType()){
      case INSTANCE:
        InstanceBinding ib = (InstanceBinding) b;
        if(ib.getInstance().getAnnotation(RequestScoped.class) != null){
          bind(ib.getInstance()).in(RequestScoped.class).to(b.getIface());
        }else{
          bind(ib.getInstance()).to(b.getIface());
        }
        break;
      case SINGLETON:
        SingletonBinding sb = (SingletonBinding) b;
        bind(sb.getSingleton()).to(b.getIface());
        break;
      case PROVIDER:
        BinderImpl.ProviderBinding pb = (BinderImpl.ProviderBinding) b;
        bindFactory(() -> pb.getProvider().get()).to(b.getIface());
        break;
      default:
        throw new IllegalStateException();
      }
    }

    bindToSelf(QueryExecutor.class);
    bindToSelf(SampleDataPopulator.class);
    bindToSelf(DatasetVersionMutator.class);
    bindToSelf(SourceService.class);
    bindToSelf(ReflectionServiceHelper.class);
    bindToSelf(CatalogServiceHelper.class);
    bindToSelf(CollaborationHelper.class);
    bindToSelf(FormatTools.class);
    bindFactory(OptionManagerFactory.class).to(OptionManager.class);
    bind(JobsBasedRecommender.class).to(JoinRecommender.class);
    bind(DACSecurityContext.class).in(RequestScoped.class).to(SecurityContext.class);
    bindFactory(TokenInfo.Factory.class).proxy(true).in(RequestScoped.class).to(TokenInfo.class);
    bindFactory(CatalogFactory.class).proxy(true).in(RequestScoped.class).to(Catalog.class);
    bindFactory(CatalogFactory.class).proxy(true).in(RequestScoped.class).to(EntityExplorer.class);
    bindFactory(CatalogFactory.class).proxy(true).in(RequestScoped.class).to(DatasetCatalog.class);
    bindFactory(CatalogFactory.class).proxy(true).in(RequestScoped.class).to(SourceCatalog.class);
    bindReflectionFactory();
  }

  protected void bindReflectionFactory() {
    bindFactory(ReflectionAdministrationServiceFactory.class).proxy(true).in(RequestScoped.class).to(ReflectionAdministrationService.class);
  }

  private <T> ClassBinding<T> bindToSelf(Class<T> serviceType) {
    return bind(serviceType).to(serviceType);
  }

  /**
   * Factory for Catalog creation.
   */
  public static class CatalogFactory implements Supplier<Catalog> {
    private final CatalogService catalogService;
    private final SecurityContext context;

    @Inject
    public CatalogFactory(CatalogService catalogService, SecurityContext context) {
      super();
      this.catalogService = catalogService;
      this.context = context;
    }

    @Override
    public Catalog get() {
      return catalogService.getCatalog(MetadataRequestOptions.of(
          SchemaConfig.newBuilder(context.getUserPrincipal().getName())
              .build()));
    }
  }

  /**
   * A factory for OptionManager to be used for DI
   * (and avoid passing directly SabotContext...)
   */
  public static class OptionManagerFactory implements Supplier<OptionManager> {
    private final SabotContext context;

    @Inject
    public OptionManagerFactory(SabotContext context) {
      this.context = context;
    }

    @Override
    public OptionManager get() {
      return context.getOptionManager();
    }
  }

  /**
   * Factory
   */
  public static class ReflectionAdministrationServiceFactory implements Supplier<ReflectionAdministrationService> {
    private final ReflectionAdministrationService.Factory factory;
    private final SecurityContext securityContext;

    @Inject
    public ReflectionAdministrationServiceFactory(ReflectionAdministrationService.Factory reflectionAdministrationServiceFactory,
                                                  SecurityContext securityContext) {
      this.factory = reflectionAdministrationServiceFactory;
      this.securityContext = securityContext;
    }

    @Override
    public ReflectionAdministrationService get() {
      return factory.get(new ReflectionContext(securityContext.getUserPrincipal().getName(), true));
    }
  }
}
