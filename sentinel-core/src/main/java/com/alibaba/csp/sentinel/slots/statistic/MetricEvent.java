/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.slots.statistic;

import com.alibaba.csp.sentinel.CtSph;
import com.alibaba.csp.sentinel.slots.block.flow.controller.DefaultController;

/**
 * @author Eric Zhao
 * 指标类型，例如通过数量、阻塞数量、异常数量、成功数量、响应时间等。
 */
public enum MetricEvent {

    /**
     * Normal pass.
     * 通过数量
     */
    PASS,
    /**
     * Normal block.
     * 阻塞数量
     */
    BLOCK,
    /**
     * 异常数量，业务异常，即捕获到非BlockException
     */
    EXCEPTION,
    /**
     * 成功数量
     * @see StatisticSlot#recordCompleteFor(com.alibaba.csp.sentinel.node.Node, int, long, java.lang.Throwable)
     * 中记录，说明不管是否发生BlockException或者其他Exception都会记录此数值，
     * 含义应该是Sentinel，执行
     * @see CtSph#entryWithPriority(com.alibaba.csp.sentinel.slotchain.ResourceWrapper, int, boolean, java.lang.Object...)
     * 方法的次数
     */
    SUCCESS,
    /**
     * 响应时间，通SUCCESS逻辑
     */
    RT,

    /**
     * Passed in future quota (pre-occupied, since 1.5.0).
     * @see DefaultController#canPass(com.alibaba.csp.sentinel.node.Node, int, boolean)
     * 申请未来令牌通过的个数
     */
    OCCUPIED_PASS
}
