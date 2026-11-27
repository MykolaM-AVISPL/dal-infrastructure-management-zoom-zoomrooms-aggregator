/*
 * Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.aggregator;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;

import com.avispl.symphony.api.dal.dto.monitor.EndpointStatistics;
import com.avispl.symphony.api.dal.dto.monitor.Statistics;
import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;
import com.avispl.symphony.api.dal.monitor.aggregator.PollContext;
import com.avispl.symphony.api.dal.monitor.aggregator.PollResult;
import com.avispl.symphony.api.dal.webhook.WebhookEvent;

/**
 * ZoomRoomsHostPolledTest
 * <p>
 * The host-driven cycle ({@link com.avispl.symphony.api.dal.monitor.aggregator.HostPolled}): no threads of its own, one cycle for concurrent
 * callers, detached results, interruption and deadline, and the lazy CommProxy loader.
 *
 * @author Mykola.Matsishin / Symphony Dev Team<br>
 * Created on 2026-11-23
 * @since 1.2.10
 */
@Tag("Mock")
public class ZoomRoomsHostPolledTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String roomId = "r" + UUID.randomUUID();
    private final String roomUserId = "u" + UUID.randomUUID();
    private final String slowUserPrefix = "slow-" + UUID.randomUUID() + "-";
    private final ExecutorService executor = Executors.newFixedThreadPool(4);
    private FakeZoom zoom;

    @BeforeEach
    public void setUp() throws Exception {
        zoom = new FakeZoom();
        zoom.setExcludePropertyGroups("RoomUserDetails,RoomDevices,RoomControlSettings");
        zoom.init();
    }

    @AfterEach
    public void tearDown() {
        zoom.destroy();
        executor.shutdownNow();
    }

    @Test
    public void constructor_shouldStartNoThreads() throws Exception {
        Set<Thread> before = new HashSet<>(Thread.getAllStackTraces().keySet());

        FakeZoom fresh = new FakeZoom();
        fresh.init();
        Set<Thread> started = new HashSet<>(Thread.getAllStackTraces().keySet());
        started.removeAll(before);
        fresh.destroy();

        Assertions.assertEquals(Collections.emptySet(), started);
    }

    @Test
    public void poll_shouldReportRoomsWithMetrics() throws Exception {
        PollResult result = zoom.poll(context(PollContext.Reason.SCHEDULE, 30));

        Assertions.assertTrue(result.isComplete());
        Assertions.assertEquals(1, result.getDevices().size());
        AggregatedDevice room = result.getDevices().get(0);
        Assertions.assertEquals(roomId, room.getDeviceId());
        Assertions.assertEquals("10.0.0.1", room.getProperties().get("Metrics#DeviceIp"));
        Assertions.assertEquals(Boolean.FALSE, inCall(room));
        Assertions.assertEquals(1, result.getAggregatorStatistics().size());
    }

    @Test
    public void poll_shouldCallVendorOnce_whenCalledConcurrently() throws Exception {
        zoom.holdRooms = new CountDownLatch(1);
        List<AtomicReference<PollResult>> results = new ArrayList<>();
        List<Thread> callers = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            AtomicReference<PollResult> result = new AtomicReference<>();
            results.add(result);
            Thread caller = new Thread(() -> {
                try {
                    result.set(zoom.poll(context(PollContext.Reason.SCHEDULE, 30)));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            callers.add(caller);
            caller.start();
            if (i == 0) {
                Assertions.assertTrue(zoom.roomsRequested.await(5, TimeUnit.SECONDS), "first cycle reaches the rooms request");
            }
        }
        TimeUnit.MILLISECONDS.sleep(300);

        zoom.holdRooms.countDown();
        for (Thread caller : callers) {
            caller.join(5000);
        }

        Assertions.assertEquals(1, zoom.roomsRequests.get());
        for (AtomicReference<PollResult> result : results) {
            Assertions.assertSame(results.get(0).get(), result.get());
        }
    }

    @Test
    public void poll_shouldKeepEarlierResult_whenWebhookChangesTheRoom() throws Exception {
        PollResult scheduled = zoom.poll(context(PollContext.Reason.SCHEDULE, 30));
        int requests = zoom.requests.get();

        zoom.digest(Collections.singletonList(meetingStarted(roomUserId)));

        Assertions.assertEquals(Boolean.FALSE, inCall(scheduled.getDevices().get(0)), "an earlier result is never changed");
        Assertions.assertEquals(requests, zoom.requests.get(), "digest makes no vendor requests");
    }

    @Test
    public void poll_shouldStopAndKeepInterrupt_whenThreadInterrupted() throws Exception {
        zoom.holdMetrics = new CountDownLatch(1);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        AtomicBoolean interruptKept = new AtomicBoolean();
        Thread caller = new Thread(() -> {
            try {
                zoom.poll(context(PollContext.Reason.SCHEDULE, 30));
            } catch (Throwable e) {
                thrown.set(e);
                interruptKept.set(Thread.currentThread().isInterrupted());
            }
        });
        caller.start();
        Assertions.assertTrue(zoom.metricsRequested.await(5, TimeUnit.SECONDS));

        caller.interrupt();
        caller.join(5000);

        Assertions.assertFalse(caller.isAlive());
        Assertions.assertTrue(thrown.get() instanceof InterruptedException, "thrown: " + thrown.get());
        Assertions.assertTrue(interruptKept.get());
    }

    @Test
    public void poll_shouldReturnIncomplete_whenDeadlinePasses() throws Exception {
        zoom.setExcludePropertyGroups("RoomDevices,RoomControlSettings");
        zoom.slowRooms = 1;

        long started = System.currentTimeMillis();
        PollResult result = zoom.poll(new PollContext(executor, Instant.now().plusMillis(700), PollContext.Reason.SCHEDULE));

        Assertions.assertFalse(result.isComplete());
        Assertions.assertTrue(System.currentTimeMillis() - started < 3000, "the cycle ends near its deadline, not after the slow room");
        Assertions.assertEquals(1, result.getDevices().size(), "the slow room the deadline cut is left out");
        Assertions.assertEquals(roomId, result.getDevices().get(0).getDeviceId());
        Assertions.assertEquals(2, result.getListed().size(), "both rooms are listed");
    }

    @Test
    public void poll_shouldStopSubmittingRooms_whenDeadlinePassesOnABoundedExecutor() throws Exception {
        zoom.setExcludePropertyGroups("RoomDevices,RoomControlSettings");
        zoom.slowRooms = 6;
        Semaphore permits = new Semaphore(1);
        Executor bounded = task -> {
            permits.acquireUninterruptibly();
            executor.execute(() -> {
                try {
                    task.run();
                } finally {
                    permits.release();
                }
            });
        };

        long started = System.currentTimeMillis();
        PollResult result = zoom.poll(new PollContext(bounded, Instant.now().plusMillis(500), PollContext.Reason.SCHEDULE));

        Assertions.assertFalse(result.isComplete());
        Assertions.assertTrue(System.currentTimeMillis() - started < 3000, "rooms not started by the deadline are skipped, not queued behind the slow ones");
    }

    @Test
    public void destroy_shouldWaitForTheRunningCycle() throws Exception {
        zoom.holdRooms = new CountDownLatch(1);
        Thread caller = new Thread(() -> {
            try {
                zoom.poll(context(PollContext.Reason.SCHEDULE, 30));
            } catch (Exception e) {
                // interrupted by destroy
            }
        });
        caller.start();
        Assertions.assertTrue(zoom.roomsRequested.await(5, TimeUnit.SECONDS));
        CompletableFuture<?> cycle = ((AtomicReference<CompletableFuture<?>>) field("cycleInFlight")).get();

        zoom.destroy();

        Assertions.assertTrue(cycle.isDone(), "destroy returns only after the cycle stopped writing");
    }

    @Test
    public void putExtraRequestHeaders_shouldRequestOneToken_whenCalledConcurrently() throws Exception {
        List<Thread> callers = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Thread caller = new Thread(() -> {
                try {
                    zoom.putExtraRequestHeaders(HttpMethod.GET, "rooms", new HttpHeaders());
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            callers.add(caller);
            caller.start();
        }
        for (Thread caller : callers) {
            caller.join(5000);
        }

        Assertions.assertEquals(1, zoom.tokenRequests.get());
    }

    @Test
    public void retrieveMultipleStatistics_shouldStartLoaderLazily() throws Exception {
        Assertions.assertNull(field("executorService"), "no loader before CommProxy asks for statistics");

        zoom.retrieveMultipleStatistics();

        Assertions.assertNotNull(field("executorService"));
    }

    // ---- helpers

    private PollContext context(PollContext.Reason reason, int seconds) {
        return new PollContext(executor, Instant.now().plusSeconds(seconds), reason);
    }

    private static WebhookEvent meetingStarted(String hostId) {
        String body = "{\"event\":\"meeting.started\",\"event_ts\":" + System.currentTimeMillis() + ",\"payload\":{\"object\":{\"id\":\"m1\",\"host_id\":\""
                + hostId + "\"}}}";
        return new WebhookEvent(1, new Date(), body);
    }

    private static Boolean inCall(AggregatedDevice room) {
        List<Statistics> statistics = room.getMonitoredStatistics();
        Assertions.assertNotNull(statistics, "monitored statistics set");
        return statistics.stream().filter(s -> s instanceof EndpointStatistics).map(s -> ((EndpointStatistics) s).isInCall()).findFirst()
                .orElseThrow(() -> new AssertionError("no EndpointStatistics"));
    }

    @SuppressWarnings("unchecked")
    private <T> T field(String name) throws Exception {
        Field f = ZoomRoomsAggregatorCommunicator.class.getDeclaredField(name);
        f.setAccessible(true);
        return (T) f.get(zoom);
    }

    /** Answers rooms, metrics and user details without a network; counts requests and can hold them. */
    private class FakeZoom extends ZoomRoomsAggregatorCommunicator {
        final AtomicInteger requests = new AtomicInteger();
        final AtomicInteger roomsRequests = new AtomicInteger();
        final AtomicInteger tokenRequests = new AtomicInteger();
        final CountDownLatch roomsRequested = new CountDownLatch(1);
        final CountDownLatch metricsRequested = new CountDownLatch(1);
        volatile CountDownLatch holdRooms;
        volatile CountDownLatch holdMetrics;
        volatile int slowRooms;

        FakeZoom() throws IOException {
            setLogin("clientId");
            setPassword("clientSecret");
            setAccountId("accountId");
            setHost("api.zoom.us");
            setProtocol("https");
            setPort(443);
        }

        @Override
        protected <Request, Response> Response doPost(String url, Request request, Class<Response> responseClass) throws Exception {
            tokenRequests.incrementAndGet();
            TimeUnit.MILLISECONDS.sleep(100);
            return JSON.readValue("{\"access_token\": \"token\", \"expires_in\": 3599}", responseClass);
        }

        @Override
        protected <Response> Response doGet(String url, Class<Response> responseClass) throws Exception {
            requests.incrementAndGet();
            if (url.startsWith("rooms?")) {
                roomsRequests.incrementAndGet();
                roomsRequested.countDown();
                await(holdRooms);
                StringBuilder slow = new StringBuilder();
                for (int i = 0; i < slowRooms; i++) {
                    slow.append(",{\"id\":\"").append(slowUserPrefix).append(i).append("\",\"room_id\":\"r").append(slowUserPrefix).append(i)
                            .append("\",\"name\":\"Slow\",\"status\":\"Available\"}");
                }
                return responseClass.cast(JSON.readTree("{\"rooms\":[{\"id\":\"" + roomUserId + "\",\"room_id\":\"" + roomId
                        + "\",\"name\":\"Board Room\",\"status\":\"Available\"}" + slow + "],\"next_page_token\":\"\"}"));
            }
            if (url.startsWith("metrics/zoomrooms?")) {
                metricsRequested.countDown();
                await(holdMetrics);
                return responseClass.cast(JSON.readTree("{\"zoom_rooms\":[{\"id\":\"" + roomId
                        + "\",\"device_ip\":\"10.0.0.1\",\"status\":\"Available\"}],\"next_page_token\":\"\"}"));
            }
            if (url.startsWith("users/")) {
                if (url.contains(slowUserPrefix)) {
                    TimeUnit.SECONDS.sleep(slowRooms > 1 ? 1 : 5);
                }
                return responseClass.cast(JSON.readTree("{\"role_name\":\"Member\"}"));
            }
            throw new IllegalArgumentException("unexpected GET " + url);
        }

        private void await(CountDownLatch latch) throws InterruptedException {
            if (latch != null) {
                latch.await();
            }
        }
    }
}
