/*
 * Licensed to Crate.io Inc. (Crate) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file to
 * you under the Apache License, Version 2.0 (the "License");  you may not
 * use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, to use any modules in this file marked as "Enterprise Features",
 * Crate must have given you permission to enable and use such Enterprise
 * Features and you must have a valid Enterprise or Subscription Agreement
 * with Crate.  If you enable or use the Enterprise Features, you represent
 * and warrant that you have a valid Enterprise or Subscription Agreement
 * with Crate.  Your use of the Enterprise Features if governed by the terms
 * and conditions of your Enterprise or Subscription Agreement with Crate.
 */

package io.crate.operation.aggregation;

import io.crate.analyze.symbol.Aggregation;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.Input;

import java.util.Locale;

/**
 * A wrapper around an AggregationFunction that is aware of the aggregation steps (iter, partial, final)
 * and will call the correct functions on the aggregationFunction depending on these steps.
 */
public class Aggregator {

    private final Input[] inputs;
    private final AggregationFunction aggregationFunction;
    private final FromImpl fromImpl;
    private final ToImpl toImpl;

    public Aggregator(RamAccountingContext ramAccountingContext,
                      Aggregation a,
                      AggregationFunction aggregationFunction,
                      Input... inputs) {
        if (a.fromStep() == Aggregation.Step.PARTIAL && inputs.length > 1) {
            throw new UnsupportedOperationException("Aggregation from PARTIAL is only allowed with one input.");
        }

        switch (a.fromStep()) {
            case ITER:
                fromImpl = new FromIter(ramAccountingContext);
                break;
            case PARTIAL:
                fromImpl = new FromPartial(ramAccountingContext);
                break;
            case FINAL:
                throw new UnsupportedOperationException("Can't start from FINAL");
            default:
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "invalid from step %s", a.fromStep().name()));
        }

        switch (a.toStep()) {
            case ITER:
                throw new UnsupportedOperationException("Can't aggregate to ITER");
            case PARTIAL:
                toImpl = new ToPartial(ramAccountingContext);
                break;
            case FINAL:
                toImpl = new ToFinal(ramAccountingContext);
                break;
            default:
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "invalid to step %s", a.toStep().name()));
        }

        this.inputs = inputs;
        this.aggregationFunction = aggregationFunction;
    }


    public Object prepareState() {
        return fromImpl.prepareState();
    }

    public Object processRow(Object value) {
        return fromImpl.processRow(value);
    }

    public Object finishCollect(Object state) {
        return toImpl.finishCollect(state);
    }

    abstract class FromImpl {

        protected final RamAccountingContext ramAccountingContext;

        public FromImpl(RamAccountingContext ramAccountingContext) {
            this.ramAccountingContext = ramAccountingContext;
        }

        public Object prepareState() {
            return aggregationFunction.newState(ramAccountingContext);
        }

        public abstract Object processRow(Object value);
    }

    class FromIter extends FromImpl {

        public FromIter(RamAccountingContext ramAccountingContext) {
            super(ramAccountingContext);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object processRow(Object value) {
            return aggregationFunction.iterate(ramAccountingContext, value, inputs);
        }
    }

    class FromPartial extends FromImpl {

        public FromPartial(RamAccountingContext ramAccountingContext) {
            super(ramAccountingContext);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object processRow(Object value) {
            return aggregationFunction.reduce(ramAccountingContext, value, inputs[0].value());
        }
    }

    static abstract class ToImpl {
        protected final RamAccountingContext ramAccountingContext;

        public ToImpl(RamAccountingContext ramAccountingContext) {
            this.ramAccountingContext = ramAccountingContext;
        }

        public abstract Object finishCollect(Object state);
    }

    class ToPartial extends ToImpl {

        public ToPartial(RamAccountingContext ramAccountingContext) {
            super(ramAccountingContext);
        }

        @Override
        public Object finishCollect(Object state) {
            return state;
        }
    }

    class ToFinal extends ToImpl {

        public ToFinal(RamAccountingContext ramAccountingContext) {
            super(ramAccountingContext);
        }

        @Override
        public Object finishCollect(Object state) {
            //noinspection unchecked
            return aggregationFunction.terminatePartial(ramAccountingContext, state);
        }
    }
}
