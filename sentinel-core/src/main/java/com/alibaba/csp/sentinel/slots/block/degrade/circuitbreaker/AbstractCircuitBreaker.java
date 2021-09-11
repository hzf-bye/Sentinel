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

import java.util.concurrent.atomic.AtomicReference;

import com.alibaba.csp.sentinel.Entry;
import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRuleManager;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeSlot;
import com.alibaba.csp.sentinel.util.AssertUtil;
import com.alibaba.csp.sentinel.util.TimeUtil;
import com.alibaba.csp.sentinel.util.function.BiConsumer;

/**
 * @author Eric Zhao
 * @since 1.8.0
 */
public abstract class AbstractCircuitBreaker implements CircuitBreaker {

    /**
     * 降级规则
     */
    protected final DegradeRule rule;

    /**
     * 降级发生后多久进行恢复，即结束降级，单位为秒。
     * 超时后，将进入半开状态，通过一个请求，以判断是否恢复。
     * {@link AbstractCircuitBreaker#fromOpenToHalfOpen(com.alibaba.csp.sentinel.context.Context)}
     * 只有一个请求能进入半开状态
     */
    protected final int recoveryTimeoutMs;

    /**
     * 事件观察者，单例的。
     * 可以注册一些事件，监听状态的变更
     */
    private final EventObserverRegistry observerRegistry;

    /**
     * 当前熔断器的状态
     * OPEN:断路器开着的拒绝所有请求
     * CLOSE:断路器关着的允许所有请求
     * HALF-OPEN:半开状态，断路器将允许“探测”调用。如果“探测”调用调用异常（例如很慢），则断路器 将重新转换为OPEN状态并等待下一个恢复时间点；否则资源将被视为“已恢复”并且断路器 *将停止切断请求并转换为CLOSE状态。
     */
    protected final AtomicReference<State> currentState = new AtomicReference<>(State.CLOSED);
    /**
     * 降级后的恢复时间，=降级时候的时间戳+recoveryTimeoutMs
     */
    protected volatile long nextRetryTimestamp;


    public AbstractCircuitBreaker(DegradeRule rule) {
        this(rule, EventObserverRegistry.getInstance());
    }

    AbstractCircuitBreaker(DegradeRule rule, EventObserverRegistry observerRegistry) {
        AssertUtil.notNull(observerRegistry, "observerRegistry cannot be null");
        if (!DegradeRuleManager.isValidRule(rule)) {
            throw new IllegalArgumentException("Invalid DegradeRule: " + rule);
        }
        this.observerRegistry = observerRegistry;
        this.rule = rule;
        this.recoveryTimeoutMs = rule.getTimeWindow() * 1000;
    }

    @Override
    public DegradeRule getRule() {
        return rule;
    }

    @Override
    public State currentState() {
        return currentState.get();
    }

    @Override
    public boolean tryPass(Context context) {
        // Template implementation.
        //如果断路器是关着的那么直接返回成功
        if (currentState.get() == State.CLOSED) {
            return true;
        }
        //断路器开着的
        if (currentState.get() == State.OPEN) {
            // For half-open state we allow a request for probing.
            /*
             * 1. retryTimeoutArrived() 判断降级后的恢复时间是否到达
             * 2. fromOpenToHalfOpen(context)，到这儿说明当天已经过了降级后的恢复时间，那么允许一个线程设置为半开状态，并注册钩子handler
             *
             */
            return retryTimeoutArrived() && fromOpenToHalfOpen(context);
        }
        return false;
    }

    /**
     * Reset the statistic data.
     */
    abstract void resetStat();

    protected boolean retryTimeoutArrived() {
        return TimeUtil.currentTimeMillis() >= nextRetryTimestamp;
    }

    protected void updateNextRetryTimestamp() {
        this.nextRetryTimestamp = TimeUtil.currentTimeMillis() + recoveryTimeoutMs;
    }

    /**
     * snapshotValue表示触发断路器时的阈值
     * 表示异常数或者异常率或者RT
     */
    protected boolean fromCloseToOpen(double snapshotValue) {
        State prev = State.CLOSED;
        if (currentState.compareAndSet(prev, State.OPEN)) {
            updateNextRetryTimestamp();

            notifyObservers(prev, State.OPEN, snapshotValue);
            return true;
        }
        return false;
    }

    protected boolean fromOpenToHalfOpen(Context context) {
        //只允许一个请求进入半开状态
        if (currentState.compareAndSet(State.OPEN, State.HALF_OPEN)) {
            //通知注册的观察者，从OPEN态至HALF_OPEN态
            notifyObservers(State.OPEN, State.HALF_OPEN, null);
            Entry entry = context.getCurEntry();
            //注册handler退出的时候执行
            entry.whenTerminate(new BiConsumer<Context, Entry>() {
                @Override
                public void accept(Context context, Entry entry) {
                    // Note: This works as a temporary workaround for https://github.com/alibaba/Sentinel/issues/1638
                    // Without the hook, the circuit breaker won't recover from half-open state in some circumstances
                    // when the request is actually blocked by upcoming rules (not only degrade rules).
                    /**
                     * issue 1638的描述
                     * 当请求实际上被即将到来的规则阻止时，断路器不会从半开状态恢复。
                     * 比如同一个资源有两个降级规则：R1（断路器状态=OPEN，recoveryTimeout=10s）和R2（断路器状态=OPEN，recoveryTimeout=20s）
                     * 可能会出现R1已经到了恢复时间点但是R2没有。
                     * 如果有请求来了，就会有转换：R1(OPEN → HALF-OPEN){@link AbstractCircuitBreaker#retryTimeoutArrived()}会返回true, R2(OPEN)， 这个请求会被R1允许，但被R2拒绝，从而最终被阻塞。
                     * 调用实际上不会发生，所以它永远不会完成。对于从 HALF-OPEN 到 OPEN/CLOSED 的状态转换，它应该只在调用完成时发生，因此对于 R1，
                     * 关联的断路器状态将永远是 HALF-OPEN。实际上，当 R1 之后有任何阻止的规则（不仅是降级规则）时，可能会发生这种情况。
                     * 即在方法{@link DegradeSlot#exit(com.alibaba.csp.sentinel.context.Context, com.alibaba.csp.sentinel.slotchain.ResourceWrapper, int, java.lang.Object...)}
                     * 中判断curEntry.getBlockError() != null后就直接返回了，说明断路器永远是HALF-OPEN状态
                     * 因此在这里加个钩子函数当有BlockException时，如果还是HALF_OPEN态那么应该设置为OPEN态
                     */
                    if (entry.getBlockError() != null) {
                        //当前请求发生BlockException后从HALF_OPEN->OPEN
                        // Fallback to OPEN due to detecting request is blocked
                        currentState.compareAndSet(State.HALF_OPEN, State.OPEN);
                        //通知注册的观察者，从HALF_OPEN态至OPEN态
                        notifyObservers(State.HALF_OPEN, State.OPEN, 1.0d);
                    }
                }
            });
            return true;
        }
        return false;
    }

    private void notifyObservers(CircuitBreaker.State prevState, CircuitBreaker.State newState, Double snapshotValue) {
        for (CircuitBreakerStateChangeObserver observer : observerRegistry.getStateChangeObservers()) {
            observer.onStateChange(prevState, newState, rule, snapshotValue);
        }
    }

    protected boolean fromHalfOpenToOpen(double snapshotValue) {
        if (currentState.compareAndSet(State.HALF_OPEN, State.OPEN)) {
            updateNextRetryTimestamp();
            notifyObservers(State.HALF_OPEN, State.OPEN, snapshotValue);
            return true;
        }
        return false;
    }

    protected boolean fromHalfOpenToClose() {
        if (currentState.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
            resetStat();
            notifyObservers(State.HALF_OPEN, State.CLOSED, null);
            return true;
        }
        return false;
    }

    protected void transformToOpen(double triggerValue) {
        State cs = currentState.get();
        switch (cs) {
            case CLOSED:
                fromCloseToOpen(triggerValue);
                break;
            case HALF_OPEN:
                fromHalfOpenToOpen(triggerValue);
                break;
            default:
                break;
        }
    }
}
