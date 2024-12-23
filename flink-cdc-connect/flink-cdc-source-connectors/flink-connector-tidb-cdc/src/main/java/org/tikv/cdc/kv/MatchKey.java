package org.tikv.cdc.kv;

import java.util.Objects;

public class MatchKey {
    private long startTs;
    private String key;

    public MatchKey(long startTs, String key) {
        this.startTs = startTs;
        this.key = key;
    }

    public long getStartTs() {
        return startTs;
    }

    public String getKey() {
        return key;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        MatchKey matchKey = (MatchKey) obj;
        return startTs == matchKey.startTs && key.equals(matchKey.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTs, key);
    }
}
