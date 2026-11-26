/*
 * Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */
package com.avispl.symphony.dal.communicator.aggregator.webhook;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * One Zoom webhook delivery, parsed.
 * <p>
 * Format as documented by Zoom (meeting.ended is the same, with the same {@code object} fields):
 *
 * <pre>
 * {"event":"meeting.started","event_ts":1626230691572,"payload":{"account_id":"...","object":{"id":"1234567890",
 *  "uuid":"...","host_id":"...","topic":"My Meeting","type":8,"start_time":"2021-07-13T21:44:51Z",
 *  "timezone":"America/Los_Angeles","duration":60}}}
 * </pre>
 *
 * The event names no room: the adapter matches {@code object.host_id} against the rooms' user ids, so only meetings
 * hosted by a room's own user are attributed to that room.
 * <p>
 * Zoom Room events ({@code zoomroom.alert}, {@code zoomroom.delayed_alert}, {@code zoomroom.created}, {@code .updated}, {@code .deleted}) name
 * the room in {@code object.id}; an alert also carries {@code issue} and {@code alert_time}:
 *
 * <pre>
 * {"event":"zoomroom.alert","event_ts":1626230691572,"payload":{"account_id":"...","object":{"id":"...","room_name":"Board Room",
 *  "issue":"Zoom room is offline","alert_time":"2021-07-13T21:44:50Z","component":"Zoom Room","alert_kind":"Triggered"}}}
 * </pre>
 * <p>
 * {@code endpoint.url_validation} (Zoom's URL challenge) is recognised but cannot be answered here: Zoom expects the
 * HMAC response in the HTTP reply to its POST, which only the receiving service (dal-webhook) can give.
 * <p>
 * Bodies are untrusted internet input: parsed as a plain JSON tree only (no type information, no binding to classes).
 */
public final class ZoomWebhookEvent {

    public enum Type {
        MEETING_STARTED, MEETING_ENDED, PARTICIPANT_JOINED, PARTICIPANT_LEFT, URL_VALIDATION,
        /** zoomroom.alert or zoomroom.delayed_alert: the same payload, the delayed one five minutes later */
        ROOM_ALERT, ROOM_CREATED, ROOM_UPDATED, ROOM_DELETED, UNKNOWN
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** The alert issues that say the room went offline or came back, e.g. "Zoom room is offline". */
    private static final Pattern ROOM_ONLINE_ISSUE = Pattern.compile("zoom room is (offline|online)\\.?", Pattern.CASE_INSENSITIVE);

    private final Type type;
    private final String rawType;
    /** {@code object.host_id}: the meeting host's user id, or {@code null} */
    private final String hostId;
    /** {@code object.id}: the meeting id of meeting events, the room's id of zoomroom events */
    private final String objectId;
    /** {@code object.participant}'s user id for participant events, or {@code null} */
    private final String participantUserId;
    /** vendor event time, or {@code null} when absent / unparseable */
    private final Instant occurredAt;
    /** {@code object.issue} of an alert, or {@code null} */
    private final String issue;
    /** {@code object.alert_time} of an alert, or {@code null} when absent / unparseable */
    private final Instant alertTime;

    private ZoomWebhookEvent(Type type, String rawType, String hostId, String objectId, String participantUserId, Instant occurredAt, String issue,
            Instant alertTime) {
        this.type = type;
        this.rawType = rawType;
        this.hostId = hostId;
        this.objectId = objectId;
        this.participantUserId = participantUserId;
        this.occurredAt = occurredAt;
        this.issue = issue;
        this.alertTime = alertTime;
    }

    /**
     * @param body raw delivery body
     * @return the parsed event
     * @throws IllegalArgumentException if the body is not a JSON object
     */
    public static ZoomWebhookEvent parse(String body) {
        JsonNode root;
        try {
            root = MAPPER.readTree(body == null ? "" : body);
        } catch (Exception e) {
            throw new IllegalArgumentException("not JSON: " + e.getMessage(), e);
        }
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("not a JSON object");
        }
        String rawType = text(root, "event");
        Type type;
        if ("meeting.started".equals(rawType)) {
            type = Type.MEETING_STARTED;
        } else if ("meeting.ended".equals(rawType)) {
            type = Type.MEETING_ENDED;
        } else if ("meeting.participant_joined".equals(rawType)) {
            type = Type.PARTICIPANT_JOINED;
        } else if ("meeting.participant_left".equals(rawType)) {
            type = Type.PARTICIPANT_LEFT;
        } else if ("endpoint.url_validation".equals(rawType)) {
            type = Type.URL_VALIDATION;
        } else if ("zoomroom.alert".equals(rawType) || "zoomroom.delayed_alert".equals(rawType)) {
            type = Type.ROOM_ALERT;
        } else if ("zoomroom.created".equals(rawType)) {
            type = Type.ROOM_CREATED;
        } else if ("zoomroom.updated".equals(rawType)) {
            type = Type.ROOM_UPDATED;
        } else if ("zoomroom.deleted".equals(rawType)) {
            type = Type.ROOM_DELETED;
        } else {
            type = Type.UNKNOWN;
        }
        JsonNode object = root.path("payload").path("object");

        Instant occurredAt = epochMillis(root.get("event_ts"));
        if (occurredAt == null) {
            occurredAt = instant(text(object, type == Type.MEETING_ENDED ? "end_time" : "start_time"));
        }
        JsonNode participant = object.path("participant");
        // shortcut: participant_user_id, else id, per Zoom's docs; confirm against a live event which one carries the room's user id
        String participantUserId = text(participant, "participant_user_id");
        if (participantUserId == null || participantUserId.isEmpty()) {
            participantUserId = text(participant, "id");
        }
        JsonNode alertTime = object.get("alert_time");
        Instant alertAt = alertTime != null && alertTime.isTextual() ? instant(alertTime.asText()) : epochMillis(alertTime);
        return new ZoomWebhookEvent(type, rawType, text(object, "host_id"), text(object, "id"), participantUserId, occurredAt, text(object, "issue"),
                alertAt);
    }

    /** A number of epoch millis as an instant, or {@code null}. */
    private static Instant epochMillis(JsonNode node) {
        return node != null && node.isNumber() && node.canConvertToLong() ? Instant.ofEpochMilli(node.asLong()) : null;
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return v == null || v.isNull() || !v.isValueNode() ? null : v.asText();
    }

    private static Instant instant(String iso) {
        if (iso == null) {
            return null;
        }
        try {
            return Instant.parse(iso);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    public Type getType() {
        return type;
    }

    public String getRawType() {
        return rawType;
    }

    public String getHostId() {
        return hostId;
    }

    public String getMeetingId() {
        return objectId;
    }

    /**
     * Retrieves {@code objectId}: {@code object.id}, the room's id (its room id or its user id) for zoomroom events.
     */
    public String getObjectId() {
        return objectId;
    }

    public String getIssue() {
        return issue;
    }

    public Instant getAlertTime() {
        return alertTime;
    }

    /**
     * What an alert's issue says about the room: {@code false} for "Zoom room is offline", {@code true} for "Zoom room is online" (any case,
     * optional trailing period), {@code null} for any other issue.
     */
    public Boolean getRoomOnline() {
        if (issue == null) {
            return null;
        }
        Matcher matcher = ROOM_ONLINE_ISSUE.matcher(issue.trim());
        return matcher.matches() ? "online".equalsIgnoreCase(matcher.group(1)) : null;
    }

    public String getParticipantUserId() {
        return participantUserId;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }
}
