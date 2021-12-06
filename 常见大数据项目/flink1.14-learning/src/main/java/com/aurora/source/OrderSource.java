/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aurora.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Random;

public class OrderSource extends RichParallelSourceFunction<Order> {

    private final int count;

    public OrderSource(int count) {
        this.count = count;
    }

    @Override
    public void run(SourceContext<Order> sourceContext) throws Exception {
        Random random = new Random();
        for (int i = 0; i < count; ++i) {
            Order order = new Order();
            order.setOrderNumber(Math.abs(random.nextLong()));
            order.setPrice(new BigDecimal("2.52"));
            order.setName("name_" + i);
            order.setType(1 + random.nextInt(5));
            long now = System.currentTimeMillis();
            order.setOrderTime(
                    LocalDateTime.ofEpochSecond(
                            now / 1000, ((int) (now % 1000) * 1000000), ZoneOffset.UTC));
            sourceContext.collect(order);
            Thread.sleep(80);
        }
    }

    @Override
    public void cancel() {}
}
