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

package org.apache.flink.cdc.debezium.rate;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.RateLimiter;

import java.io.Serializable;

/**
 * A concrete implementation of the DebeziumRateLimiter interface that uses a token bucket algorithm
 * to limit the rate of event processing. This implementation is based on the Guava RateLimiter.
 */
public class TokenRateLimiter implements DebeziumRateLimiter, Serializable {

    /**
     * The rate limiter instance that controls the rate of event processing. It uses a token bucket
     * algorithm to enforce the rate limit.
     */
    private RateLimiter rateLimiter;

    private final Rate rate;

    /**
     * Constructs a new TokenRateLimiter with the specified rate configuration.
     *
     * @param rate the rate configuration that defines the maximum number of permits per time unit.
     */
    public TokenRateLimiter(Rate rate) {
        this.rate = rate;
    }

    /**
     * Resets the rate limiter to the initial configuration. This method is useful if the rate limit
     * needs to be reconfigured dynamically.
     */
    public void resetRate() {
        this.rateLimiter = RateLimiter.create(rate.getMaxPerCount());
    }

    /**
     * Attempts to acquire a permit to process an event. This method will block until a permit is
     * available, or until the thread is interrupted. It also ensures that the rate limiter is
     * initialized if it is null.
     *
     * @return the time in seconds that the caller should wait before attempting to acquire again.
     *     This value is provided by the underlying rate limiter and can be used to implement a
     *     backoff strategy.
     */
    @Override
    public double acquire() {
        if (rateLimiter == null) {
            resetRate();
        }
        return rateLimiter.acquire();
    }

    /**
     * Checks whether the rate limiter is enabled. The rate limiter is considered enabled if the
     * maximum number of permits per time unit is greater than zero.
     *
     * @return true if the rate limiter is enabled, false otherwise.
     */
    @Override
    public boolean isEnable() {
        return rate.getMaxPerCount() > 0;
    }
}
