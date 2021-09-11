/*
 * Copyright 1999-2019 Alibaba Group Holding Ltd.
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
package com.alibaba.csp.sentinel.slots.block.degrade.circuitbreaker;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import com.alibaba.csp.sentinel.Entry;
import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.statistic.base.LeapArray;
import com.alibaba.csp.sentinel.slots.statistic.base.WindowWrap;
import com.alibaba.csp.sentinel.util.AssertUtil;

import static com.alibaba.csp.sentinel.slots.block.RuleConstant.DEGRADE_GRADE_EXCEPTION_COUNT;
import static com.alibaba.csp.sentinel.slots.block.RuleConstant.DEGRADE_GRADE_EXCEPTION_RATIO;

/**
 * @author Eric Zhao
 * @since 1.8.0
 * 异常比例或者异数降级策略类
 */
public class ExceptionCircuitBreaker extends AbstractCircuitBreaker {

    /**
     * Circuit breaking strategy (0: average RT, 1: exception ratio, 2: exception count).
     * 熔断策略
     * 0-RT
     * 1-异常比例 {@link com.alibaba.csp.sentinel.node.IntervalProperty#INTERVAL}时间内的异常率
     * 2-异常数,过去60s的异常数与count比较
     * @see com.alibaba.csp.sentinel.slots.block.RuleConstant#DEGRADE_GRADE_EXCEPTION_RATIO
     * @see com.alibaba.csp.sentinel.slots.block.RuleConstant#DEGRADE_GRADE_EXCEPTION_COUNT
     * 在这里值为1或者2
     */
    private final int strategy;
    /**
     * 熔断触发的最小请求数，比如请求数小于该值时即使异常比率超出阈值也不会熔断
     */
    private final int minRequestAmount;

    /**
     * 规则中对应的配置值
     * strategy=1时表示 错误率 0-1之间
     * strategy=2时表示 错误数
     */
    private final double threshold;

    /**
     * 错误数的滑动窗口的实现类，默认60s统计一次，即时间周期是60s，窗口的间隔是60s
     */
    private final LeapArray<SimpleErrorCounter> stat;

    public ExceptionCircuitBreaker(DegradeRule rule) {
        this(rule, new SimpleErrorCounterLeapArray(1, rule.getStatIntervalMs()));
    }

    ExceptionCircuitBreaker(DegradeRule rule, LeapArray<SimpleErrorCounter> stat) {
        super(rule);
        this.strategy = rule.getGrade();
        boolean modeOk = strategy == DEGRADE_GRADE_EXCEPTION_RATIO || strategy == DEGRADE_GRADE_EXCEPTION_COUNT;
        AssertUtil.isTrue(modeOk, "rule strategy should be error-ratio or error-count");
        AssertUtil.notNull(stat, "stat cannot be null");
        this.minRequestAmount = rule.getMinRequestAmount();
        this.threshold = rule.getCount();
        this.stat = stat;
    }

    @Override
    protected void resetStat() {
        // Reset current bucket (bucket count = 1).
        stat.currentWindow().value().reset();
    }

    @Override
    public void onRequestComplete(Context context) {
        Entry entry = context.getCurEntry();
        if (entry == null) {
            return;
        }
        Throwable error = entry.getError();
        //获取当前时间窗口的统计数据
        SimpleErrorCounter counter = stat.currentWindow().value();
        if (error != null) {
            //发生系统异常那么错误数+1
            counter.getErrorCount().add(1);
        }
        //总的请求数+1
        counter.getTotalCount().add(1);

        //超过设定的阈值的情况下状态变更
        handleStateChangeWhenThresholdExceeded(error);
    }

    /**
     * 超过设定的阈值的情况下状态变更
     */
    private void handleStateChangeWhenThresholdExceeded(Throwable error) {
        //断路器开着的直接返回。
        if (currentState.get() == State.OPEN) {
            return;
        }


        if (currentState.get() == State.HALF_OPEN) {
            // In detecting request
            //如果是在半开状态未发生异常了那么进入CLOSE态
            if (error == null) {
                fromHalfOpenToClose();
            } else {

                //如果是在半开状态仍然发生异常了那么进入OPEN态
                fromHalfOpenToOpen(1.0d);
            }
            return;
        }

        //如果是CLOSE态那么计算阈值

        List<SimpleErrorCounter> counters = stat.values();
        long errCount = 0;
        long totalCount = 0;
        //获取当前时间所在的时间窗口的统计数据
        for (SimpleErrorCounter counter : counters) {
            errCount += counter.errorCount.sum();
            totalCount += counter.totalCount.sum();
        }
        //小于最小请求数忽略
        if (totalCount < minRequestAmount) {
            return;
        }
        double curCount = errCount;
        if (strategy == DEGRADE_GRADE_EXCEPTION_RATIO) {
            // Use errorRatio
            //计算异常率
            curCount = errCount * 1.0d / totalCount;
        }
        //异常数或者异常率超过阈值
        if (curCount > threshold) {
            //设置为OPEN态，并更新恢复时间
            transformToOpen(curCount);
        }
    }

    static class SimpleErrorCounter {
        private LongAdder errorCount;
        private LongAdder totalCount;

        public SimpleErrorCounter() {
            this.errorCount = new LongAdder();
            this.totalCount = new LongAdder();
        }

        public LongAdder getErrorCount() {
            return errorCount;
        }

        public LongAdder getTotalCount() {
            return totalCount;
        }

        public SimpleErrorCounter reset() {
            errorCount.reset();
            totalCount.reset();
            return this;
        }

        @Override
        public String toString() {
            return "SimpleErrorCounter{" +
                "errorCount=" + errorCount +
                ", totalCount=" + totalCount +
                '}';
        }
    }

    static class SimpleErrorCounterLeapArray extends LeapArray<SimpleErrorCounter> {

        public SimpleErrorCounterLeapArray(int sampleCount, int intervalInMs) {
            super(sampleCount, intervalInMs);
        }

        @Override
        public SimpleErrorCounter newEmptyBucket(long timeMillis) {
            return new SimpleErrorCounter();
        }

        @Override
        protected WindowWrap<SimpleErrorCounter> resetWindowTo(WindowWrap<SimpleErrorCounter> w, long startTime) {
            // Update the start time and reset value.
            w.resetTo(startTime);
            w.value().reset();
            return w;
        }
    }
}
