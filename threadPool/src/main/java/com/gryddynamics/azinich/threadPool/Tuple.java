package com.gryddynamics.azinich.threadPool;

import java.util.Map;

class Tuple implements Map.Entry<Runnable, Long> {

    private Runnable key;
    private Long value;

    Tuple(Runnable key, Long value) {
        this.key = key;
        this.value = value;
    }

    public Runnable getKey() {
        return key;
    }

    public Long getValue() {
        return value;
    }

    public Long setValue(Long value) {
        this.value = value;
        return value;
    }

    public Runnable setKey(Runnable key) {
        this.key = key;
        return key;
    }
}
