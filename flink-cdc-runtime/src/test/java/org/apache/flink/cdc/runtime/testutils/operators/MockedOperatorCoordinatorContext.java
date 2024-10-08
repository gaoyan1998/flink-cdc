/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.runtime.testutils.operators;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;

/**
 * This is a mocked version of Operator coordinator context that stores failure cause for testing
 * purposes only.
 */
public class MockedOperatorCoordinatorContext extends MockOperatorCoordinatorContext {
    public MockedOperatorCoordinatorContext(
            OperatorID operatorID, ClassLoader userCodeClassLoader) {
        super(operatorID, userCodeClassLoader);
    }

    private Throwable failureCause;

    @Override
    public void failJob(Throwable cause) {
        super.failJob(cause);
        failureCause = cause;
    }

    public Throwable getFailureCause() {
        return failureCause;
    }
}
