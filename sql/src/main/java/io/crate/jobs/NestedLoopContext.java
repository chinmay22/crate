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

package io.crate.jobs;

import io.crate.operation.join.NestedLoopOperation;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import org.elasticsearch.common.logging.ESLogger;

import javax.annotation.Nullable;

public class NestedLoopContext extends AbstractExecutionSubContext implements DownstreamExecutionSubContext {

    private final NestedLoopPhase nestedLoopPhase;

    @Nullable
    private final PageBucketReceiver leftBucketReceiver;
    @Nullable
    private final PageBucketReceiver rightBucketReceiver;
    private final RowReceiver leftRowReceiver;
    private final RowReceiver rightRowReceiver;

    public NestedLoopContext(ESLogger logger,
                             NestedLoopPhase nestedLoopPhase,
                             NestedLoopOperation nestedLoopOperation,
                             @Nullable PageBucketReceiver leftBucketReceiver,
                             @Nullable PageBucketReceiver rightBucketReceiver) {
        super(nestedLoopPhase.phaseId(), logger);

        this.nestedLoopPhase = nestedLoopPhase;
        this.leftBucketReceiver = leftBucketReceiver;
        this.rightBucketReceiver = rightBucketReceiver;

        leftRowReceiver = nestedLoopOperation.leftRowReceiver();
        rightRowReceiver = nestedLoopOperation.rightRowReceiver();

        nestedLoopOperation.completionFuture().whenComplete((Object result, Throwable t) -> {
            if (t == null) {
                future.close(null);
            } else {
                future.close(t);
            }
        });
    }

    @Override
    public String name() {
        return nestedLoopPhase.name();
    }

    @Override
    public int id() {
        return nestedLoopPhase.phaseId();
    }

    @Override
    protected void innerClose(@Nullable Throwable t) {
        closeReceiver(t, leftBucketReceiver, leftRowReceiver);
        closeReceiver(t, rightBucketReceiver, rightRowReceiver);
    }

    private static void closeReceiver(@Nullable Throwable t, @Nullable PageBucketReceiver subContext, RowReceiver rowReceiver) {
        if (subContext == null && t != null) {
            rowReceiver.fail(t);
        }
    }

    @Override
    protected void innerKill(@Nullable Throwable t) {
        killReceiver(t, leftBucketReceiver, leftRowReceiver);
        killReceiver(t, rightBucketReceiver, rightRowReceiver);
    }

    private static void killReceiver(Throwable t, @Nullable PageBucketReceiver subContext, RowReceiver rowReceiver) {
        if (subContext == null) {
            rowReceiver.kill(t);
        }
    }

    @Override
    public PageBucketReceiver getBucketReceiver(byte inputId) {
        assert inputId < 2 : "Only 0 and 1 inputId's supported";
        if (inputId == 0) {
            return leftBucketReceiver;
        }
        return rightBucketReceiver;
    }

    @Override
    public String toString() {
        return "NestedLoopContext{" +
               "id=" + id() +
               ", leftCtx=" + leftBucketReceiver +
               ", rightCtx=" + rightBucketReceiver +
               ", closed=" + future.closed() +
               '}';
    }

}
