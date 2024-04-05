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
package com.dremio.sabot.driver;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.op.spi.BatchStreamProvider;
import com.dremio.sabot.op.spi.DualInputOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.op.spi.SingleInputOperator;
import com.dremio.sabot.op.spi.TerminalOperator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unchecked")
public class OperatorCreatorRegistry implements OperatorCreator {

  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(OperatorCreatorRegistry.class);

  @SuppressWarnings("rawtypes")
  private final ImmutableMap<Class<?>, SingleInputOperator.Creator> singleInputCreators;

  @SuppressWarnings("rawtypes")
  private final ImmutableMap<Class<?>, DualInputOperator.Creator> dualInputCreators;

  @SuppressWarnings("rawtypes")
  private final ImmutableMap<Class<?>, ProducerOperator.Creator> producerCreators;

  @SuppressWarnings("rawtypes")
  private final ImmutableMap<Class<?>, ProducerOperator.ReceiverCreator> receiverCreators;

  @SuppressWarnings("rawtypes")
  private final ImmutableMap<Class<?>, TerminalOperator.Creator> terminalCreators;

  public OperatorCreatorRegistry(ScanResult scanResult) {
    this.singleInputCreators = getImplementors(scanResult, SingleInputOperator.Creator.class);
    this.dualInputCreators = getImplementors(scanResult, DualInputOperator.Creator.class);
    this.producerCreators = getImplementors(scanResult, ProducerOperator.Creator.class);
    this.receiverCreators = getImplementors(scanResult, ProducerOperator.ReceiverCreator.class);
    this.terminalCreators = getImplementors(scanResult, TerminalOperator.Creator.class);
  }

  @Override
  public <T extends PhysicalOperator> SingleInputOperator getSingleInputOperator(
      OperatorContext context, T operator) throws ExecutionSetupException {
    SingleInputOperator.Creator<T> creator =
        (SingleInputOperator.Creator<T>) singleInputCreators.get(operator.getClass());
    Preconditions.checkNotNull(
        creator,
        "Unable to find creator for operator of type %s with configuration %s",
        operator.getClass().getName(),
        operator.toString());
    return creator.create(context, (T) operator);
  }

  @Override
  public <T extends PhysicalOperator> SingleInputOperator getSingleInputOperator(
      FragmentExecutionContext fec, OperatorContext context, T operator)
      throws ExecutionSetupException {
    SingleInputOperator.Creator<T> creator =
        (SingleInputOperator.Creator<T>) singleInputCreators.get(operator.getClass());
    Preconditions.checkNotNull(
        creator,
        "Unable to find creator for operator of type %s with configuration %s",
        operator.getClass().getName(),
        operator.toString());
    return creator.create(fec, context, (T) operator);
  }

  @Override
  public <T extends PhysicalOperator> DualInputOperator getDualInputOperator(
      OperatorContext context, T operator) throws ExecutionSetupException {
    DualInputOperator.Creator<T> creator =
        (DualInputOperator.Creator<T>) dualInputCreators.get(operator.getClass());
    Preconditions.checkNotNull(
        creator,
        "Unable to find creator for operator of type %s with configuration %s",
        operator.getClass().getName(),
        operator.toString());
    return creator.create(context, (T) operator);
  }

  @Override
  public <T extends PhysicalOperator> TerminalOperator getTerminalOperator(
      TunnelProvider provider, OperatorContext context, T operator) throws ExecutionSetupException {
    TerminalOperator.Creator<T> creator =
        (TerminalOperator.Creator<T>) terminalCreators.get(operator.getClass());
    Preconditions.checkNotNull(
        creator,
        "Unable to find creator for operator of type %s with configuration %s",
        operator.getClass().getName(),
        operator.toString());
    return creator.create(provider, context, (T) operator);
  }

  @Override
  public <T extends PhysicalOperator> ProducerOperator getProducerOperator(
      FragmentExecutionContext fec, OperatorContext context, T operator)
      throws ExecutionSetupException {
    ProducerOperator.Creator<T> creator =
        (ProducerOperator.Creator<T>) producerCreators.get(operator.getClass());
    Preconditions.checkNotNull(
        creator,
        "Unable to find creator for operator of type %s with configuration %s",
        operator.getClass().getName(),
        operator.toString());
    return creator.create(fec, context, (T) operator);
  }

  @Override
  public <T extends PhysicalOperator> ProducerOperator getReceiverOperator(
      BatchStreamProvider buffers, OperatorContext context, T operator)
      throws ExecutionSetupException {
    ProducerOperator.ReceiverCreator<T> creator =
        (ProducerOperator.ReceiverCreator<T>) receiverCreators.get(operator.getClass());
    Preconditions.checkNotNull(
        creator,
        "Unable to find creator for operator of type %s with configuration %s",
        operator.getClass().getName(),
        operator.toString());
    return creator.create(buffers, context, (T) operator);
  }

  private <T> ImmutableMap<Class<?>, T> getImplementors(
      ScanResult scanResult, Class<T> baseInterface) {
    final Map<Class<?>, T> map = new HashMap<>();

    Set<Class<? extends T>> providerClasses = scanResult.getImplementations(baseInterface);
    for (Class<?> c : providerClasses) {
      Class<?> operatorClass = c;
      boolean interfaceFound = false;
      while (!interfaceFound && !(c.equals(java.lang.Object.class))) {
        final Type[] ifaces = c.getGenericInterfaces(); // never returns null
        for (Type iface : ifaces) {
          if (!(iface instanceof ParameterizedType
              && ((ParameterizedType) iface).getRawType().equals(baseInterface))) {
            continue;
          }
          final Type[] args = ((ParameterizedType) iface).getActualTypeArguments();
          interfaceFound = true;
          boolean constructorFound = false;
          for (Constructor<?> constructor : operatorClass.getConstructors()) {
            Class<?>[] params = constructor.getParameterTypes();
            if (params.length == 0) {
              try {
                T newInstance = (T) constructor.newInstance();
                Object old = map.put((Class<?>) args[0], newInstance);
                if (old != null) {
                  throw UserException.functionError()
                      .message(
                          "Duplicate OperatorCreator [%s, %s] found for PhysicalOperator %s",
                          old.getClass().getCanonicalName(),
                          operatorClass.getCanonicalName(),
                          ((Class<?>) args[0]).getCanonicalName())
                      .build(logger);
                }
                constructorFound = true;
              } catch (Exception ex) {
                logger.warn(
                    "Failure while creating OperatorCreator. Constructor declaring class {}.",
                    constructor.getDeclaringClass().getName(),
                    ex);
              }
            }
          }
          if (!constructorFound) {
            logger.debug(
                "Skipping registration of OperatorCreator {} as it doesn't have a default constructor",
                operatorClass.getCanonicalName());
          }
        }
        c = c.getSuperclass();
      }
    }
    return ImmutableMap.copyOf(map);
  }
}
