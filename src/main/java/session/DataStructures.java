package session;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;

public class DataStructures {
    public static class Sessions {
        private List<SessionEvent> sessions;

        public List<SessionEvent> getSessions() {
            return sessions;
        }

        public void setSessions(List<SessionEvent> sessions) {
            if (sessions.isEmpty()) {
                throw new IllegalArgumentException("events should not be empty!");
            }

            this.sessions = sessions;
        }

        public static Sessions newInstance(List<SessionEvent> sessions) {
            Sessions instance = new Sessions();
            instance.setSessions(sessions);
            return instance;
        }
    }

    public enum EventTypes {
        SessionStart, SessionEnd
    }

    public enum StateRecordType {
        Complete, ErrorNullStartTime, ErrorNullEndTime, Incomplete, TimeOut
    }

    public static class SessionEvent implements Serializable {
        private CompositeKey groupId;
        private Timestamp startTimestamp;

        public CompositeKey getGroupId() {
            return groupId;
        }

        public void setGroupId(CompositeKey groupId) {
            this.groupId = groupId;
        }

        private Timestamp endTimestamp;

        private StateRecordType recordType;

        public StateRecordType getRecordType() {
            return recordType;
        }

        public void setRecordType(StateRecordType recordType) {
            this.recordType = recordType;
        }

        public Timestamp getStartTimestamp() {
            return startTimestamp;
        }

        public void setStartTimestamp(Timestamp startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

        public Timestamp getEndTimestamp() {
            return endTimestamp;
        }

        public void setEndTimestamp(Timestamp endTimestamp) {
            this.endTimestamp = endTimestamp;
        }

        public static SessionEvent newInstance(CompositeKey groupId, StateRecordType recordType,
                                               Timestamp startTimestamp, Timestamp endTimestamp) {
            SessionEvent instance = new SessionEvent();
            instance.setGroupId(groupId);
            instance.setRecordType(recordType);
            instance.setStartTimestamp(startTimestamp);
            instance.setEndTimestamp(endTimestamp);
            return instance;
        }
    }


    public static class Session implements Serializable {
        private String userID;
        private String sessionID;
        private Timestamp startTimestamp;
        private Timestamp endTimestamp;

        private StateRecordType recordType;

        public String getUserID() {
            return userID;
        }

        public void setUserID(String userID) {
            this.userID = userID;
        }

        public String getSessionID() {
            return sessionID;
        }

        public void setSessionID(String sessionID) {
            this.sessionID = sessionID;
        }

        public Timestamp getStartTimestamp() {
            return startTimestamp;
        }

        public void setStartTimestamp(Timestamp startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

        public Timestamp getEndTimestamp() {
            return endTimestamp;
        }

        public StateRecordType getRecordType() {
            return recordType;
        }

        public void setRecordType(StateRecordType recordType) {
            this.recordType = recordType;
        }

        public void setEndTimestamp(Timestamp endTimestamp) {
            this.endTimestamp = endTimestamp;
        }

        public static Session newInstance(String userID, String sessionID, Timestamp startTimestamp, Timestamp endTimestamp, StateRecordType recordType) {
            Session instance = new Session();
            instance.setUserID(userID);
            instance.setSessionID(sessionID);
            instance.setStartTimestamp(startTimestamp);
            instance.setEndTimestamp(endTimestamp);
            instance.setRecordType(recordType);
            return instance;
        }
    }

    public static class SessionInput implements Serializable {
        private CompositeKey groupId;
        private EventTypes eventType;

        public CompositeKey getGroupId() {
            return groupId;
        }

        public void setGroupId(CompositeKey groupId) {
            this.groupId = groupId;
        }

        private Timestamp timeStamp;

        public Timestamp getTimeStamp() {
            return timeStamp;
        }

        public void setTimeStamp(Timestamp timeStamp) {
            this.timeStamp = timeStamp;
        }

        public EventTypes getEventType() {
            return eventType;
        }

        public void setEventType(EventTypes eventType) {
            this.eventType = eventType;
        }


        public static SessionInput newInstance(CompositeKey groupId, String eventTypeStr,
                                               Timestamp timeStamp) {
            SessionInput instance = new SessionInput();
            instance.setGroupId(groupId);
            instance.setEventType(EventTypes.valueOf(eventTypeStr));
            instance.setTimeStamp(timeStamp);

            return instance;
        }
    }
    public static class CompositeKey implements Serializable {
        private String userID;
        private String sessionID;
        public CompositeKey() {
            // Default constructor
        }

        public void setUserID(String userID) {
            this.userID = userID;
        }

        public void setSessionID(String sessionID) {
            this.sessionID = sessionID;
        }

        public CompositeKey(String userID, String sessionID) {
            this.userID = userID;
            this.sessionID = sessionID;
        }

        public String getUserID() {
            return userID;
        }

        public String getSessionID() {
            return sessionID;
        }

        // Override equals and hashCode methods
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CompositeKey) {
                CompositeKey other = (CompositeKey) obj;
                return userID.equals(other.userID) && sessionID.equals(other.sessionID);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(userID, sessionID);
        }
    }
}
