/*
 * Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.aggregator;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.avispl.symphony.api.dal.dto.monitor.EndpointStatistics;
import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;
import com.avispl.symphony.api.dal.monitor.aggregator.PollContext;
import com.avispl.symphony.api.dal.monitor.aggregator.PollResult;

/**
 * ZoomRoomsPollScopeTest
 * <p>
 * What a host-driven cycle asks Zoom about: only the subscribed rooms ({@link PollContext#getDevices()}), only the event's rooms on
 * {@link PollContext.Reason#EVENT}, the account's lists only on handshake; and when each value came from Zoom ({@code observedAt}).
 *
 * @author Mykola.Matsishin / Symphony Dev Team<br>
 * Created on 2026-11-26
 * @since 1.2.10
 */
@Tag("Mock")
public class ZoomRoomsPollScopeTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String prefix = UUID.randomUUID().toString().substring(0, 8);
    private final String room1 = "r1-" + prefix;
    private final String room2 = "r2-" + prefix;
    private final String room3 = "r3-" + prefix;
    private final ExecutorService executor = Executors.newFixedThreadPool(4);
    private FakeZoom zoom;

    @BeforeEach
    public void setUp() throws Exception {
        zoom = new FakeZoom();
        zoom.init();
    }

    @AfterEach
    public void tearDown() {
        zoom.destroy();
        executor.shutdownNow();
    }

    @Test
    public void poll_shouldRequestDetailsOnlyForSubscribedRooms_whenScheduled() throws Exception {
        PollResult result = zoom.poll(context(PollContext.Reason.SCHEDULE, room1));

        Assertions.assertEquals(Collections.singletonList(room1), ids(result));
        Assertions.assertEquals(Arrays.asList("rooms", "metrics", "users/" + user(room1), "rooms/" + room1 + "/devices"), zoom.requests());
    }

    @Test
    public void poll_shouldRequestDetailsOfEveryRoom_whenSubscriptionIsEveryDevice() throws Exception {
        PollResult result = zoom.poll(context(PollContext.Reason.SCHEDULE, (String[]) null));

        Assertions.assertEquals(new HashSet<>(Arrays.asList(room1, room2, room3)), new HashSet<>(ids(result)));
        Assertions.assertEquals(2 + 2 * 3, zoom.requests().size());
    }

    @Test
    public void poll_shouldDropRoom_whenSubscriptionShrinksDuringRoomsListCooldown() throws Exception {
        zoom.poll(context(PollContext.Reason.SCHEDULE, room1, room2));

        PollResult result = zoom.poll(context(PollContext.Reason.SCHEDULE, room1));

        Assertions.assertEquals(Collections.singletonList(room1), ids(result));
    }

    @Test
    public void poll_shouldCollectDetailsOnlyForEventRoomsItKeeps_whenEvent() throws Exception {
        zoom.poll(context(PollContext.Reason.SCHEDULE, room1, room2));
        expireCooldowns();
        zoom.clearRequests();

        PollResult result = zoom.poll(context(PollContext.Reason.EVENT, room2, room3));

        Assertions.assertEquals(Arrays.asList("rooms", "metrics", "users/" + user(room2), "rooms/" + room2 + "/devices"), zoom.requests());
        Assertions.assertEquals(new HashSet<>(Arrays.asList(room1, room2)), new HashSet<>(ids(result)), "an event poll keeps the subscription");
    }

    @Test
    public void handshake_shouldListEveryRoomWithoutRoomRequests_andLeaveThePollCacheAlone() throws Exception {
        zoom.poll(context(PollContext.Reason.SCHEDULE, room1));
        long listCooldown = field("validDeviceMetaDataRetrievalPeriodTimestamp");
        long metricsCooldown = field("validMetricsDataRetrievalPeriodTimestamp");
        zoom.clearRequests();

        PollResult inventory = zoom.handshake(context(PollContext.Reason.MANUAL, room1));

        Assertions.assertEquals(Arrays.asList("rooms", "metrics"), zoom.requests());
        Assertions.assertEquals(new HashSet<>(Arrays.asList(room1, room2, room3)), new HashSet<>(ids(inventory)));
        AggregatedDevice inMeeting = byId(inventory, room2);
        Assertions.assertEquals(user(room2), inMeeting.getSerialNumber());
        Assertions.assertEquals("Room " + room2, inMeeting.getDeviceName());
        Assertions.assertNotNull(inMeeting.getDeviceModel());
        Assertions.assertEquals(Boolean.TRUE, inMeeting.getDeviceOnline());
        Assertions.assertEquals(Boolean.TRUE, inCall(inMeeting));
        Assertions.assertEquals(Boolean.FALSE, inCall(byId(inventory, room1)));
        Assertions.assertEquals(Boolean.FALSE, byId(inventory, room3).getDeviceOnline());
        Assertions.assertEquals(Collections.singletonList(room1), ids(zoom.poll(context(PollContext.Reason.WEBHOOK, room1))));
        Assertions.assertEquals(listCooldown, (long) field("validDeviceMetaDataRetrievalPeriodTimestamp"));
        Assertions.assertEquals(metricsCooldown, (long) field("validMetricsDataRetrievalPeriodTimestamp"));
    }

    @Test
    public void poll_shouldTimeEveryGroupAndStampTheNewest_whenScheduled() throws Exception {
        long before = System.currentTimeMillis();

        PollResult result = zoom.poll(context(PollContext.Reason.SCHEDULE, room1));

        Map<String, Long> times = result.getObservedAt().get(room1);
        Assertions.assertNotNull(times, "observedAt of " + room1 + ": " + result.getObservedAt());
        for (String key : Arrays.asList("online", "inCall", "Metrics#DeviceIp", "Metrics#RoomStatus", "RoomUserDetails#RoleName")) {
            Assertions.assertTrue(times.containsKey(key), key + " in " + times);
            Assertions.assertTrue(times.get(key) >= before, key);
        }
        Assertions.assertTrue(times.keySet().stream().anyMatch(key -> key.startsWith("RoomDevicesZoomRoomsComputers#")), "RoomDevices group timed: " + times);
        Assertions.assertEquals(times.get("Metrics#RoomStatus"), times.get("inCall"), "inCall comes from the metrics page");
        Assertions.assertEquals(Collections.max(times.values()), byId(result, room1).getTimestamp());
    }

    @Test
    public void poll_shouldKeepTimesAndTimestamp_whenEveryGroupIsInCooldown() throws Exception {
        PollResult first = zoom.poll(context(PollContext.Reason.SCHEDULE, room1));
        TimeUnit.MILLISECONDS.sleep(20);

        PollResult second = zoom.poll(context(PollContext.Reason.SCHEDULE, room1));

        Assertions.assertEquals(Collections.emptyList(), zoom.requests().subList(4, zoom.requests().size()), "everything in cooldown");
        Assertions.assertEquals(first.getObservedAt().get(room1), second.getObservedAt().get(room1));
        Assertions.assertEquals(byId(first, room1).getTimestamp(), byId(second, room1).getTimestamp());
    }

    @Test
    public void poll_shouldRenewOnlyTheFetchedGroupsTimes_whenOneGroupLeavesCooldown() throws Exception {
        PollResult first = zoom.poll(context(PollContext.Reason.SCHEDULE, room1));
        TimeUnit.MILLISECONDS.sleep(20);
        this.<Map<?, ?>>field("validUserDetailsDataRetrievalPeriodTimestamps").clear();

        PollResult second = zoom.poll(context(PollContext.Reason.SCHEDULE, room1));

        Map<String, Long> before = first.getObservedAt().get(room1);
        Map<String, Long> after = second.getObservedAt().get(room1);
        Assertions.assertTrue(after.get("RoomUserDetails#RoleName") > before.get("RoomUserDetails#RoleName"));
        Assertions.assertEquals(before.get("Metrics#DeviceIp"), after.get("Metrics#DeviceIp"));
        Assertions.assertEquals(before.get("online"), after.get("online"));
        Assertions.assertEquals(after.get("RoomUserDetails#RoleName"), byId(second, room1).getTimestamp());
        Assertions.assertTrue(byId(second, room1).getTimestamp() > byId(first, room1).getTimestamp());
    }

    @Test
    public void poll_shouldReportTimesOfRoomsOnly_whenWebhookSnapshot() throws Exception {
        zoom.poll(context(PollContext.Reason.SCHEDULE, room1));

        PollResult snapshot = zoom.poll(context(PollContext.Reason.WEBHOOK, room1));

        Assertions.assertEquals(Collections.singleton(room1), snapshot.getObservedAt().keySet());
        Assertions.assertEquals(Collections.max(snapshot.getObservedAt().get(room1).values()), byId(snapshot, room1).getTimestamp());
    }

    @Test
    public void poll_shouldCollectTheRoomWithTheOldestDetailsFirst() throws Exception {
        zoom.poll(context(PollContext.Reason.SCHEDULE, room1, room2));

        for (String oldest : Arrays.asList(room1, room2)) {
            expireCooldowns();
            Map<String, Long> collectedAt = field("detailsCollectedAt");
            collectedAt.put(room1, oldest.equals(room1) ? 1L : 2L);
            collectedAt.put(room2, oldest.equals(room2) ? 1L : 2L);
            zoom.clearRequests();

            // one room at a time, in the order the adapter hands them over
            zoom.poll(new PollContext(Runnable::run, Instant.now().plusSeconds(30), PollContext.Reason.SCHEDULE, set(room1, room2)));

            Assertions.assertEquals("users/" + user(oldest), zoom.requests().get(2), "the first room request of " + zoom.requests());
        }
    }

    @Test
    public void poll_shouldReportSubscribedRoomDevices_withOneDevicesRequestPerRoom_whenIncludeRoomDevices() throws Exception {
        zoom.setIncludeRoomDevices(true);

        PollResult result = zoom.poll(context(PollContext.Reason.SCHEDULE, room1, device(room1)));

        Assertions.assertEquals(Arrays.asList("rooms", "metrics", "users/" + user(room1), "rooms/" + room1 + "/devices"), zoom.requests());
        Assertions.assertEquals(set(room1, device(room1)), new HashSet<>(ids(result)));
        AggregatedDevice roomDevice = byId(result, device(room1));
        Assertions.assertEquals(Boolean.TRUE, roomDevice.getDeviceOnline());
        Map<String, Long> times = result.getObservedAt().get(device(room1));
        Assertions.assertNotNull(times, "observedAt of " + device(room1) + ": " + result.getObservedAt());
        Assertions.assertTrue(times.containsKey("online"), "online timed: " + times);
        Assertions.assertEquals(Collections.max(times.values()), roomDevice.getTimestamp());
    }

    @Test
    public void poll_shouldLeaveOutRoomDevicesNotSubscribed_whenIncludeRoomDevices() throws Exception {
        zoom.setIncludeRoomDevices(true);

        PollResult result = zoom.poll(context(PollContext.Reason.SCHEDULE, room1));

        Assertions.assertEquals(Collections.singletonList(room1), ids(result));
    }

    @Test
    public void poll_shouldDropRoomDevice_whenItsRoomLeavesTheSubscription() throws Exception {
        zoom.setIncludeRoomDevices(true);
        zoom.poll(context(PollContext.Reason.SCHEDULE, room1, device(room1)));

        PollResult result = zoom.poll(context(PollContext.Reason.SCHEDULE, room2, device(room1)));

        Assertions.assertEquals(Collections.singletonList(room2), ids(result));
        Assertions.assertFalse(result.getObservedAt().containsKey(device(room1)));
    }

    @Test
    public void poll_shouldTimeRoomDevicesInCallByTheirRoom_whenRoomDevicesFollowTheCall() throws Exception {
        zoom.setIncludeRoomDevices(true);
        zoom.setIncludeRoomDevicesInCalls(true);

        PollResult result = zoom.poll(context(PollContext.Reason.SCHEDULE, room2, device(room2)));

        AggregatedDevice roomDevice = byId(result, device(room2));
        Assertions.assertEquals(Boolean.TRUE, inCall(roomDevice));
        Assertions.assertEquals(result.getObservedAt().get(room2).get("inCall"), result.getObservedAt().get(device(room2)).get("inCall"));
        Assertions.assertEquals(Collections.max(result.getObservedAt().get(device(room2)).values()), roomDevice.getTimestamp());
    }

    @Test
    public void handshake_shouldListTheDevicesOfTheGivenRoomsOnly_whenIncludeRoomDevices() throws Exception {
        zoom.setIncludeRoomDevices(true);

        PollResult inventory = zoom.handshake(context(PollContext.Reason.MANUAL, room1));

        Assertions.assertEquals(Arrays.asList("rooms", "metrics", "rooms/" + room1 + "/devices"), zoom.requests());
        Assertions.assertEquals(set(room1, room2, room3, device(room1)), new HashSet<>(ids(inventory)));
        Map<String, Long> times = inventory.getObservedAt().get(device(room1));
        Assertions.assertEquals(Collections.max(times.values()), byId(inventory, device(room1)).getTimestamp());
    }

    // ---- helpers

    private static Set<String> set(String... ids) {
        return new HashSet<>(Arrays.asList(ids));
    }

    /** The one device the fake lists in a room. */
    private static String device(String roomId) {
        return "d-" + roomId;
    }

    private PollContext context(PollContext.Reason reason, String... devices) {
        Set<String> subscription = devices == null ? null : new HashSet<>(Arrays.asList(devices));
        return new PollContext(executor, Instant.now().plusSeconds(30), reason, subscription);
    }

    private static String user(String roomId) {
        return "u-" + roomId;
    }

    private static List<String> ids(PollResult result) {
        return result.getDevices().stream().map(AggregatedDevice::getDeviceId).collect(Collectors.toList());
    }

    private static AggregatedDevice byId(PollResult result, String deviceId) {
        return result.getDevices().stream().filter(device -> deviceId.equals(device.getDeviceId())).findFirst()
                .orElseThrow(() -> new AssertionError("no " + deviceId));
    }

    private static Boolean inCall(AggregatedDevice room) {
        Assertions.assertNotNull(room.getMonitoredStatistics(), "monitored statistics of " + room.getDeviceId());
        return room.getMonitoredStatistics().stream().filter(s -> s instanceof EndpointStatistics).map(s -> ((EndpointStatistics) s).isInCall())
                .findFirst().orElseThrow(() -> new AssertionError("no EndpointStatistics"));
    }

    private void expireCooldowns() throws Exception {
        setField("validDeviceMetaDataRetrievalPeriodTimestamp", 0L);
        setField("validMetricsDataRetrievalPeriodTimestamp", 0L);
        this.<Map<?, ?>>field("validUserDetailsDataRetrievalPeriodTimestamps").clear();
        this.<Map<?, ?>>field("validRoomDevicesDataRetrievalPeriodTimestamps").clear();
    }

    @SuppressWarnings("unchecked")
    private <T> T field(String name) throws Exception {
        Field f = ZoomRoomsAggregatorCommunicator.class.getDeclaredField(name);
        f.setAccessible(true);
        return (T) f.get(zoom);
    }

    private void setField(String name, Object value) throws Exception {
        Field f = ZoomRoomsAggregatorCommunicator.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(zoom, value);
    }

    /** Three rooms: room1 available, room2 in a meeting, room3 offline; logs every GET by its path without the query. */
    private class FakeZoom extends ZoomRoomsAggregatorCommunicator {
        private final List<String> requests = Collections.synchronizedList(new ArrayList<>());

        FakeZoom() throws IOException {
            setLogin("clientId");
            setPassword("clientSecret");
            setAccountId("accountId");
            setHost("api.zoom.us");
            setProtocol("https");
            setPort(443);
        }

        List<String> requests() {
            synchronized (requests) {
                return new ArrayList<>(requests);
            }
        }

        void clearRequests() {
            requests.clear();
        }

        @Override
        protected <Request, Response> Response doPost(String url, Request request, Class<Response> responseClass) throws Exception {
            return JSON.readValue("{\"access_token\": \"token\", \"expires_in\": 3599}", responseClass);
        }

        @Override
        protected <Response> Response doGet(String url, Class<Response> responseClass) throws Exception {
            String path = url.split("\\?")[0];
            if (path.equals("rooms")) {
                requests.add("rooms");
                return responseClass.cast(JSON.readTree("{\"rooms\":[" + room(room1, "Available") + "," + room(room2, "InMeeting") + ","
                        + room(room3, "Offline") + "],\"next_page_token\":\"\"}"));
            }
            if (path.equals("metrics/zoomrooms")) {
                requests.add("metrics");
                return responseClass.cast(JSON.readTree("{\"zoom_rooms\":[" + metrics(room1, "Available") + "," + metrics(room2, "In Meeting") + ","
                        + metrics(room3, "Offline") + "],\"next_page_token\":\"\"}"));
            }
            requests.add(path);
            if (path.startsWith("users/")) {
                return responseClass.cast(JSON.readTree("{\"role_name\":\"Member\",\"email\":\"" + path + "@example.com\"}"));
            }
            if (path.startsWith("rooms/") && path.endsWith("/devices")) {
                return responseClass.cast(JSON.readTree("{\"devices\":[{\"id\":\"d-" + path.split("/")[1] + "\",\"device_type\":\"Zoom Rooms Computer\","
                        + "\"app_version\":\"5.0\",\"device_system\":\"Win 10\",\"status\":\"Online\"}],\"next_page_token\":\"\"}"));
            }
            throw new IllegalArgumentException("unexpected GET " + url);
        }

        private String room(String roomId, String status) {
            return "{\"id\":\"" + user(roomId) + "\",\"room_id\":\"" + roomId + "\",\"name\":\"Room " + roomId + "\",\"status\":\"" + status + "\"}";
        }

        private String metrics(String roomId, String status) {
            return "{\"id\":\"" + roomId + "\",\"device_ip\":\"10.0.0.1\",\"status\":\"" + status + "\"}";
        }
    }
}
