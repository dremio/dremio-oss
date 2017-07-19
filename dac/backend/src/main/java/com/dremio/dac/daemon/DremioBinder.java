/*
 * Copyright (C) 2017 Dremio Corporation
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

import javax.ws.rs.core.SecurityContext;

import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.hk2.utilities.binding.ServiceBindingBuilder;
import org.glassfish.jersey.process.internal.RequestScoped;

import com.dremio.dac.explore.QueryExecutor;
import com.dremio.dac.explore.join.JobsBasedRecommender;
import com.dremio.dac.explore.join.JoinRecommender;
import com.dremio.dac.server.DACSecurityContext;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.dac.service.datasets.DatasetVersionMutator;
import com.dremio.dac.service.source.SourceService;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaTreeProvider;
import com.dremio.service.BinderImpl.Binding;
import com.dremio.service.BinderImpl.InstanceBinding;
import com.dremio.service.BinderImpl.SingletonBinding;
import com.dremio.service.SingletonRegistry;

/**
 * Class to bind resources to HK2 injector.
 */
public class DremioBinder extends AbstractBinder {

  private final SingletonRegistry bindings;

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
        if(ib.getInstance().getClass().getAnnotation(RequestScoped.class) != null){
          bind(ib.getInstance()).in(RequestScoped.class).to(b.getIface());
        }else{
          bind(ib.getInstance()).to(b.getIface());
        }
        break;
      case SINGLETON:
        SingletonBinding sb = (SingletonBinding) b;
        bind(sb.getSingleton()).to(b.getIface());
        break;
      default:
        throw new IllegalStateException();
      }
    }

    bindToSelf(QueryExecutor.class);
    bindToSelf(SampleDataPopulator.class);
    bindToSelf(DatasetVersionMutator.class);
    bindToSelf(SourceService.class);
    bind(JobsBasedRecommender.class).to(JoinRecommender.class);
    bind(DACSecurityContext.class).in(RequestScoped.class).to(SecurityContext.class);
    bindFactory(new SchemaTreeFactory()).in(RequestScoped.class).to(SchemaTreeProvider.class);
  }

  private <T> ServiceBindingBuilder<T> bindToSelf(Class<T> serviceType) {
    return bind(serviceType).to(serviceType);
  }

  private final class SchemaTreeFactory implements Factory<SchemaTreeProvider> {
    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaTreeFactory.class);
    @Override
    public SchemaTreeProvider provide() {
      return new SchemaTreeProvider(bindings.lookup(SabotContext.class));
    }

    @Override
    public void dispose(SchemaTreeProvider instance) {
      // no-op
    }

  }
}
