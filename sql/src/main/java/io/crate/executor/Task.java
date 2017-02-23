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

package io.crate.executor;

import io.crate.data.Row;
import io.crate.operation.projectors.RowReceiver;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Task {

    /**
     * execute the task if it represents a single operation.
     * <p>
     * The result will be fed into the RowReceiver.
     */
    void execute(RowReceiver rowReceiver, Row parameters);

    /**
     * execute the task if it represents a bulk operation.
     * <p>
     * The result will be a List containing the row-counts per operation.
     * Elements of the list cannot be null, but will be -1 if unknown and -2 if an error occurred.
     *
     * @throws UnsupportedOperationException if the task doesn't support bulk operations
     */
    List<CompletableFuture<Long>> executeBulk();
}
