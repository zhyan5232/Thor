package com.thor.node.network.protocol;

import java.io.Serializable;

public class ThorMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int TYPE_JSON_CMD = 0x01;
    public static final int TYPE_FILE_STREAM = 0x02;

    private int type;
    private int length;
    private byte[] payload;

    public ThorMessage(int type, byte[] payload) {
        this.type = type;
        this.payload = payload;
        this.length = (payload != null) ? payload.length : 0;
    }

    public int getType() { return type; }
    public void setType(int type) { this.type = type; }

    public int getLength() { return length; }
    public void setLength(int length) { this.length = length; }

    public byte[] getPayload() { return payload; }
    public void setPayload(byte[] payload) {
        this.payload = payload;
        if (payload != null) this.length = payload.length;
    }
}