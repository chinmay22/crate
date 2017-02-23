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

package io.crate.executor.transport.executionphases;

import com.google.common.util.concurrent.SettableFuture;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Triggers {@link #future} after {@code numServer} calls to either
 * {@link #jobInitialized()} or {@link #jobInitializationFailed(Throwable)}
 * <p>
 *     If {@link #jobInitializationFailed(Throwable)}} has been called at least once
 *     the future will emit a failure.
 * </p>
 */
class InitializationTracker {

    final SettableFuture<Void> future;
    private final AtomicInteger serverToInitialize;
    private volatile Throwable failure;

    InitializationTracker(int numServer) {
        future = SettableFuture.create();
        if (numServer == 0) {
            future.set(null);
        }
        serverToInitialize = new AtomicInteger(numServer);
    }

    void jobInitialized() {
        if (serverToInitialize.decrementAndGet() <= 0) {
            // no need to synchronize here, since there cannot be a failure after all servers have finished
            if (failure == null) {
                future.set(null);
            } else {
                future.setException(failure);
            }
        }
    }

    /**
     * Indicates that a jobInitialization failed
     *
     * @param t The cause of the initialization failure.
     *          If no failure has been set so far or if it was an InterruptedException it is overwritten.
     */
    void jobInitializationFailed(Throwable t) {
        synchronized (this) {
            if (failure == null || failure instanceof InterruptedException) {
                failure = t;
            }
        }
        jobInitialized();
    }
}
