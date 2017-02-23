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

package io.crate.operation.projectors;

import io.crate.concurrent.CompletableFutures;
import io.crate.data.Row;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractProjector implements Projector {

    // these stateCheck classes are used to avoid having to do null-state checks in concrete implementations
    // -> upstream/downstreams are never null (unless a concrete implementions nulls them)
    private final static RowReceiver STATE_CHECK_RECEIVER = new StateCheckReceiver();

    protected RowReceiver downstream = STATE_CHECK_RECEIVER;

    @Override
    public void downstream(RowReceiver rowReceiver) {
        assert rowReceiver != null : "rowReceiver must not be null";
        this.downstream = rowReceiver;
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return downstream.completionFuture();
    }

    @Override
    public void kill(Throwable throwable) {
        downstream.kill(throwable);
    }

    @Override
    public Set<Requirement> requirements() {
        return downstream.requirements();
    }

    @Override
    public void pauseProcessed(ResumeHandle resumeable) {
        downstream.pauseProcessed(resumeable);
    }

    private static class StateCheckReceiver implements RowReceiver {

        private static final String STATE_ERROR = "downstream not set";

        @Override
        public CompletableFuture<?> completionFuture() {
            return CompletableFutures.failedFuture(new IllegalStateException(STATE_ERROR));
        }

        @Override
        public Result setNextRow(Row row) {
            throw new IllegalStateException(STATE_ERROR);
        }

        @Override
        public void pauseProcessed(ResumeHandle resumeable) {
            throw new IllegalStateException(STATE_ERROR);
        }

        @Override
        public void finish(RepeatHandle repeatHandle) {
            throw new IllegalStateException(STATE_ERROR);
        }

        @Override
        public void fail(Throwable throwable) {
            throw new IllegalStateException(STATE_ERROR);
        }

        @Override
        public void kill(Throwable throwable) {
            throw new IllegalStateException(STATE_ERROR);
        }

        @Override
        public Set<Requirement> requirements() {
            throw new IllegalStateException(STATE_ERROR);
        }
    }
}

