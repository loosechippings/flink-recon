package loosechippings.recon.domain;

import java.util.Objects;

public class Audit {

    String id;
    long timestamp;

    public Audit() {
    }

    public Audit(String id, long timestamp) {
        this.id = id;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Audit audit = (Audit) o;
        return timestamp == audit.timestamp &&
                Objects.equals(id, audit.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timestamp);
    }

    @Override
    public String toString() {
        return "Audit{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
