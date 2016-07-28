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

package org.apache.cassandra.test.microbench;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.concurrent.MonitoredTPCExecutorService;
import org.apache.cassandra.concurrent.MonitoredTPCRxScheduler;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;


/**
 * Benchmark for eventloops
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Thread)
public class EventLoopBench {
    @State(Scope.Thread)
    public static class MonitoredState {
        @Param({"1000000"})
        public int count;

        Observable<Integer> rxMonitored;

        @Setup
        public void setup() {

            Integer[] arr = new Integer[count];
            Arrays.fill(arr, 777);

            MonitoredTPCRxScheduler scheduler = new MonitoredTPCRxScheduler();

            rxMonitored = Observable.fromArray(arr).subscribeOn(scheduler.any())
                                    .observeOn(scheduler.any());
        }
        @TearDown
        public void teardown() {
            MonitoredTPCExecutorService.instance().shutdown();
        }
    }


    @State(Scope.Thread)
    public static class ExecutorState {

        @Param({"1000000"})
        public int count;

        private ExecutorService exec1;
        private DefaultEventExecutorGroup loop1;
        private DefaultEventExecutorGroup loop2;

        Observable<Integer> rx1;
        Observable<Integer> rx2;


        @Setup
        public void setup() {
            exec1 = Executors.newSingleThreadExecutor();

            Integer[] arr = new Integer[count];
            Arrays.fill(arr, 777);

            loop1 = new DefaultEventExecutorGroup(1);
            loop2 = new DefaultEventExecutorGroup(2);

            Scheduler s1 = Schedulers.from(loop1);
            Scheduler s2 = Schedulers.from(loop2);

            rx1 = Observable.fromArray(arr).subscribeOn(Schedulers.computation()).observeOn(Schedulers.computation());
            rx2 = Observable.fromArray(arr).subscribeOn(s1).observeOn(s2);
        }

        @TearDown
        public void teardown() {
            exec1.shutdown();
            loop1.shutdown();
            loop2.shutdown();
        }
    }

    static void await(int count, CountDownLatch latch) throws Exception {
        if (count < 1000) {
            while (latch.getCount() != 0) ;
        } else {
            latch.await();
        }
    }

    //@Benchmark
    public void executor(ExecutorState state, Blackhole bh) throws Exception {

        CountDownLatch cdl = new CountDownLatch(1);

        int c = state.count;
        for (int i = 0; i < c; i++) {
            int j = i;
            state.exec1.submit(() -> {
                if (j == c - 1) {
                    cdl.countDown();
                }
            });
        }

        await(c, cdl);
    }

    @Benchmark
    public void rxDefault(ExecutorState state, Blackhole bh) throws Exception {
        LatchedObserver<Integer> o = new LatchedObserver<>(bh);
        state.rx1.subscribe(o);

        await(state.count, o.latch);
    }


    @Benchmark
    public void rxNetty(ExecutorState state, Blackhole bh) throws Exception {
        LatchedObserver<Integer> o = new LatchedObserver<>(bh);
        state.rx2.subscribe(o);

        await(state.count, o.latch);
    }

    @Benchmark
    public void rxMonitored(MonitoredState state, Blackhole bh) throws Exception {
        LatchedObserver<Integer> o = new LatchedObserver<>(bh);
        state.rxMonitored.subscribe(o);

        await(state.count, o.latch);
    }


    @Benchmark
    public void monitored(MonitoredState state, Blackhole bh) throws Exception {

        CountDownLatch cdl = new CountDownLatch(1);

        MonitoredTPCExecutorService.SingleCoreExecutor e = MonitoredTPCExecutorService.instance().one(1);

        int c = state.count;
        for (int i = 0; i < c; i++) {
            int j = i;
            e.addTask(new FutureTask<>(()-> {
                if (j == c - 1)
                    cdl.countDown();
            }, null));
        }

        await(c, cdl);
    }


    @Benchmark
    public void forkjoin(ExecutorState state, Blackhole bh) throws Exception {

        CountDownLatch cdl = new CountDownLatch(1);

        ForkJoinPool fj = ForkJoinPool.commonPool();

        int c = state.count;
        for (int i = 0; i < c; i++) {
            int j = i;
            fj.submit(() -> {
                if (j == c - 1)
                {
                    cdl.countDown();
                }
            });
        }

        await(c, cdl);
    }

    public class LatchedObserver<T> extends Observer<T>
    {
        public CountDownLatch latch = new CountDownLatch(1);
        private final Blackhole bh;

        public LatchedObserver(Blackhole bh) {
            this.bh = bh;
        }

        @Override
        public void onComplete() {
            latch.countDown();
        }

        @Override
        public void onError(Throwable e) {
            latch.countDown();
        }

        @Override
        public void onNext(T t)
        {
            bh.consume(t);
        }
    }
}
