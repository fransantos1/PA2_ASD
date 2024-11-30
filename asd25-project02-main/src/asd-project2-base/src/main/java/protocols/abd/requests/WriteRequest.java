package protocols.abd.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

import java.util.UUID;

public class WriteRequest extends ProtoRequest {

    public static final short REQUEST_ID = 202;

    private final UUID opId;
    private final long key;
    private final short value;

    public WriteRequest(UUID opId, long key, short value) {
        super(REQUEST_ID);
        this.opId = opId;
        this.key = key;
        this.value = value;
    }

    public UUID getOpId(){
        return opId;
    }

    public long getKey() {
        return key;
    }

    public short getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "WriteRequest{" +
                "key1=" + key +
                ", value=" + value +
                '}';
    }
}
