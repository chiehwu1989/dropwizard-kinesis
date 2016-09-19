package io.codemonastery.dropwizard.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Event {

    private final String id;
    private final String name;
    private final String info;

    public Event(@JsonProperty("id") String id,
                 @JsonProperty("name") String name,
                 @JsonProperty("info") String info) {
        this.id = id;
        this.name = name;
        this.info = info;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getInfo() {
        return info;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Event event = (Event) o;

        return !(id != null ? !id.equals(event.id) : event.id != null) &&
                !(name != null ? !name.equals(event.name) : event.name != null) &&
                !(info != null ? !info.equals(event.info) : event.info != null);

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (info != null ? info.hashCode() : 0);
        return result;
    }
}
