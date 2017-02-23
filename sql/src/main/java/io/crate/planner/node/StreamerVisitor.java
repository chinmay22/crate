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

package io.crate.planner.node;

import io.crate.Streamer;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.types.DataTypes;

import java.util.Locale;

/**
 * Get output {@link io.crate.Streamer}s for {@link ExecutionPhase}s
 */
public class StreamerVisitor {

    private static final ExecutionPhaseStreamerVisitor EXECUTION_PHASE_STREAMER_VISITOR = new ExecutionPhaseStreamerVisitor();

    private StreamerVisitor() {
    }

    public static Streamer<?>[] streamersFromOutputs(ExecutionPhase executionPhase) {
        return EXECUTION_PHASE_STREAMER_VISITOR.process(executionPhase, null);
    }

    private static class ExecutionPhaseStreamerVisitor extends ExecutionPhaseVisitor<Void, Streamer<?>[]> {

        private static final Streamer[] COUNT_STREAMERS = new Streamer[]{DataTypes.LONG};

        @Override
        public Streamer<?>[] visitMergePhase(MergePhase phase, Void context) {
            return DataTypes.getStreamers(phase.outputTypes());
        }

        @Override
        public Streamer<?>[] visitRoutedCollectPhase(RoutedCollectPhase phase, Void context) {
            return DataTypes.getStreamers(phase.outputTypes());
        }

        @Override
        public Streamer<?>[] visitCountPhase(CountPhase phase, Void context) {
            return COUNT_STREAMERS;
        }

        @Override
        public Streamer<?>[] visitNestedLoopPhase(NestedLoopPhase phase, Void context) {
            return DataTypes.getStreamers(phase.outputTypes());
        }

        @Override
        public Streamer<?>[] visitFileUriCollectPhase(FileUriCollectPhase phase, Void context) {
            return DataTypes.getStreamers(phase.outputTypes());
        }

        @Override
        public Streamer<?>[] visitTableFunctionCollect(TableFunctionCollectPhase phase, Void context) {
            return visitRoutedCollectPhase(phase, context);
        }

        @Override
        protected Streamer<?>[] visitExecutionPhase(ExecutionPhase node, Void context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Got unsupported ExecutionNode %s", node.getClass().getName()));
        }
    }
}
