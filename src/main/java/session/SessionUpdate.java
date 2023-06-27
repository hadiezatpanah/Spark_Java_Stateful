package session;
import com.typesafe.config.Config;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;
import session.DataStructures.*;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class SessionUpdate implements FlatMapGroupsWithStateFunction<CompositeKey, Row, Sessions, Session> {
    private long timeOutDuration;
    public SessionUpdate(Config config){
        this.timeOutDuration = config.getLong("state.timeout");
    }
    private void handleTimeout(CompositeKey groupId, GroupState<Sessions> state) {
        Sessions sessions = state.get();
        List<SessionEvent> status = new ArrayList<>();
        sessions.getSessions().forEach(session -> {
            if (session.getRecordType() == StateRecordType.Incomplete) {
                SessionEvent event = SessionEvent.newInstance(
                        groupId,
                        StateRecordType.ErrorNullEndTime,
                        session.getStartTimestamp(),
                        null);
                status.add(event);
            } else {
                status.add(session);
            }
        });
        state.update(Sessions.newInstance(status));
    }

    private Iterator<Session> handleEvict(CompositeKey groupId, GroupState<Sessions> state) {
        Sessions sessions = state.get();

        List<SessionEvent> evicted = new ArrayList<>();
        List<SessionEvent> kept = new ArrayList<>();
        // Apply Watermark to remove old state
        sessions.getSessions().forEach(session -> {
            if (session.getRecordType() != StateRecordType.Incomplete) {
                evicted.add(session);
            } else {
                kept.add(session);
            }
        });

        if (kept.isEmpty()) {
            state.remove();
        } else {
            state.update(Sessions.newInstance(kept));
//            state.setTimeoutTimestamp(time);
            state.setTimeoutDuration(Duration.ofMinutes(timeOutDuration).toMillis());
        }

        return evicted.stream()
                .map(sessionAcc -> Session.newInstance(
                        groupId.getUserID(),
                        groupId.getSessionID(),
                        sessionAcc.getStartTimestamp(),
                        sessionAcc.getEndTimestamp(),
                        sessionAcc.getRecordType()
                        )
                )
                .iterator();
    }

    private void updateState(CompositeKey groupId, List<SessionInput> sessionInput, GroupState<Sessions> state) {

        SessionInput start = null;
        List<SessionEvent> status = new ArrayList<>();
        if (state.exists()) {
            List<SessionEvent> incompleteStates = state.get().getSessions();
            incompleteStates.forEach(session -> {
                if (session.getRecordType() != StateRecordType.Incomplete) {
                    status.add(session);
                }
            });
        }
        for (int curIdx = 0; curIdx < sessionInput.size(); curIdx++) {
            SessionInput curSession = sessionInput.get(curIdx);

            if (curSession.getEventType() == EventTypes.SessionStart) {
                if (start == null) {
                    start = curSession;
                } else {
                    SessionEvent event = SessionEvent.newInstance(
                            curSession.getGroupId(),
                            StateRecordType.ErrorNullEndTime,
                            curSession.getTimeStamp(),
                            null);
                    status.add(event);
                }
            } else if (curSession.getEventType() == EventTypes.SessionEnd) {
                if (start != null) {
                    SessionInput end = curSession;
                    while (curIdx + 1 < sessionInput.size() && sessionInput.get(curIdx + 1).getEventType() == EventTypes.SessionEnd) {
                        curIdx++;
                        SessionEvent event = SessionEvent.newInstance(
                                end.getGroupId(),
                                StateRecordType.ErrorNullStartTime,
                                null,
                                end.getTimeStamp());
                        end = sessionInput.get(curIdx);
                        status.add(event);
                    }
                    SessionEvent event = SessionEvent.newInstance(
                            start.getGroupId(),
                            StateRecordType.Complete,
                            start.getTimeStamp(),
                            end.getTimeStamp());
                    status.add(event);
                    start = null;
                } else {
                    SessionEvent event = SessionEvent.newInstance(
                            curSession.getGroupId(),
                            StateRecordType.ErrorNullStartTime,
                            null,
                            curSession.getTimeStamp());
                    status.add(event);
                }

            }
        }
        if (start != null) {
            SessionEvent event = SessionEvent.newInstance(
                    start.getGroupId(),
                    StateRecordType.Incomplete,
                    start.getTimeStamp(),
                    null);
            status.add(event);
        }

        // update state
        state.update(Sessions.newInstance(status));
    }

    @Override
    public Iterator<Session> call(CompositeKey groupId, Iterator<Row> events, GroupState<Sessions> state) {

         if (state.hasTimedOut() && state.exists()) {
            handleTimeout(groupId, state);
        }

        // convert each event as individual session
        Stream<Row> stream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        events,
                        Spliterator.ORDERED), false);


        List<SessionInput> sessionsFromEvents = stream.map(r -> {
            SessionInput event = SessionInput.newInstance(groupId, r.getString(2),
                    r.getTimestamp(3));
            return event;
        }).collect(Collectors.toList());


        if (sessionsFromEvents.isEmpty()) {
            return Collections.emptyIterator();
        }


        List<SessionInput> allSessions = new ArrayList<>(sessionsFromEvents);

        if (state.exists()) {
            List<SessionEvent> incompleteStates = state.get().getSessions();
            incompleteStates.forEach(session -> {
                if (session.getRecordType() == StateRecordType.Incomplete) {
                    SessionInput incompleteSession = SessionInput.newInstance(
                            session.getGroupId(),
                            EventTypes.SessionStart.toString(),
                            session.getStartTimestamp());
                    allSessions.add(incompleteSession);
                }
            });
        }

        // sort sessions via start timestamp
        allSessions.sort(Comparator.comparingLong(s -> s.getTimeStamp().getTime()));

        // Update state based on the input
        updateState(groupId, allSessions, state);

        // we still need to handle eviction here
        return handleEvict(groupId, state);
    }
}