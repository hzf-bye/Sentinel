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
package com.alibaba.csp.sentinel.slots.block.degrade;

import com.alibaba.csp.sentinel.slots.block.AbstractRule;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;

import java.util.Objects;

/**
 * <p>
 * Degrade is used when the resources are in an unstable state, these resources
 * will be degraded within the next defined time window. There are two ways to
 * measure whether a resource is stable or not:
 * </p>
 * <ul>
 * <li>
 * Average response time ({@code DEGRADE_GRADE_RT}): When
 * the average RT exceeds the threshold ('count' in 'DegradeRule', in milliseconds), the
 * resource enters a quasi-degraded state. If the RT of next coming 5
 * requests still exceed this threshold, this resource will be downgraded, which
 * means that in the next time window (defined in 'timeWindow', in seconds) all the
 * access to this resource will be blocked.
 * </li>
 * <li>
 * Exception ratio: When the ratio of exception count per second and the
 * success qps exceeds the threshold, access to the resource will be blocked in
 * the coming window.
 * </li>
 * </ul>
 *
 * @author jialiang.linjl
 * @author Eric Zhao
 */
public class DegradeRule extends AbstractRule {

    public DegradeRule() {}

    public DegradeRule(String resourceName) {
        setResource(resourceName);
    }

    /**
     * Circuit breaking strategy (0: average RT, 1: exception ratio, 2: exception count).
     * 熔断策略
     * 0-RT
     * 1-异常比例 {@link com.alibaba.csp.sentinel.node.IntervalProperty#INTERVAL}时间内的异常率
     * 2-异常数，默认过去60s的异常数与count比较
     */
    private int grade = RuleConstant.DEGRADE_GRADE_RT;

    /**
     * Threshold count.
     * 规则中对应的配置值
     *
     * strategy=0时表示 错误率 RT
     * strategy=1时表示 错误率 0-1之间
     * strategy=2时表示 错误数
     *
     */
    private double count;

    /**
     * Recovery timeout (in seconds) when circuit breaker opens. After the timeout, the circuit breaker will
     * transform to half-open state for trying a few requests.
     * 降级发生后多久进行恢复，即结束降级，单位为毫秒。
     * 超时后，将进入半开状态，通过一些请求，以判断是否恢复。
     */
    private int timeWindow;

    /**
     * Minimum number of requests (in an active statistic time span) that can trigger circuit breaking.
     * 熔断触发的最小请求数，比如请求数小于该值时即使异常比率超出阈值也不会熔断
     *
     * @since 1.7.0
     */
    private int minRequestAmount = RuleConstant.DEGRADE_DEFAULT_MIN_REQUEST_AMOUNT;

    /**
     * The threshold of slow request ratio in RT mode.
     * RT 模式下慢请求率的阈值。即slowRatioThreshold比例的请求的RT>=count那么就应该处降级了。
     */
    private double slowRatioThreshold = 1.0d;

    /**
     * 统计时长（单位为 ms），如 60*1000 代表分钟级（1.8.0 引入）
     */
    private int statIntervalMs = 1000;

    public int getGrade() {
        return grade;
    }

    public DegradeRule setGrade(int grade) {
        this.grade = grade;
        return this;
    }

    public double getCount() {
        return count;
    }

    public DegradeRule setCount(double count) {
        this.count = count;
        return this;
    }

    public int getTimeWindow() {
        return timeWindow;
    }

    public DegradeRule setTimeWindow(int timeWindow) {
        this.timeWindow = timeWindow;
        return this;
    }

    public int getMinRequestAmount() {
        return minRequestAmount;
    }

    public DegradeRule setMinRequestAmount(int minRequestAmount) {
        this.minRequestAmount = minRequestAmount;
        return this;
    }

    public double getSlowRatioThreshold() {
        return slowRatioThreshold;
    }

    public DegradeRule setSlowRatioThreshold(double slowRatioThreshold) {
        this.slowRatioThreshold = slowRatioThreshold;
        return this;
    }

    public int getStatIntervalMs() {
        return statIntervalMs;
    }

    public DegradeRule setStatIntervalMs(int statIntervalMs) {
        this.statIntervalMs = statIntervalMs;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        DegradeRule rule = (DegradeRule)o;
        return Double.compare(rule.count, count) == 0 &&
            timeWindow == rule.timeWindow &&
            grade == rule.grade &&
            minRequestAmount == rule.minRequestAmount &&
            Double.compare(rule.slowRatioThreshold, slowRatioThreshold) == 0 &&
            statIntervalMs == rule.statIntervalMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), count, timeWindow, grade, minRequestAmount,
            slowRatioThreshold, statIntervalMs);
    }

    @Override
    public String toString() {
        return "DegradeRule{" +
            "resource=" + getResource() +
            ", grade=" + grade +
            ", count=" + count +
            ", limitApp=" + getLimitApp() +
            ", timeWindow=" + timeWindow +
            ", minRequestAmount=" + minRequestAmount +
            ", slowRatioThreshold=" + slowRatioThreshold +
            ", statIntervalMs=" + statIntervalMs +
            '}';
    }
}
