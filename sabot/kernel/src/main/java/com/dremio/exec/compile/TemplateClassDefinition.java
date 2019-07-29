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
package com.dremio.exec.compile;

import java.util.concurrent.atomic.AtomicLong;

import com.dremio.exec.compile.sig.SignatureHolder;

public class TemplateClassDefinition<T>{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TemplateClassDefinition.class);

  private final Class<T> iface;
  private final Class<?> template;
  private final SignatureHolder signature;
  private static final AtomicLong classNumber = new AtomicLong(0);

  public <X extends T> TemplateClassDefinition(Class<T> iface, Class<X> template) {
    super();
    this.iface = iface;
    this.template = template;
    SignatureHolder holder = null;
    try{
      holder = SignatureHolder.getHolder(template);
    }catch(Exception ex){
      logger.error("Failure while trying to build signature holder for signature. {}", template.getName(), ex);
    }
    this.signature = holder;

  }

  public long getNextClassNumber(){
    return classNumber.getAndIncrement();
  }

  public Class<T> getExternalInterface() {
    return iface;
  }


  public String getTemplateClassName() {
    return template.getName();
  }

  public SignatureHolder getSignature(){
    return signature;
  }

  @Override
  public String toString() {
    return "TemplateClassDefinition [template=" + template + ", signature=" + signature + "]";
  }
}
