package protocols.abd.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

import java.util.UUID;

public class ReadRequest extends ProtoRequest {

    public static final short REQUEST_ID = 201;

    private final UUID opId;
    private final long key;


    public ReadRequest(UUID opId, long key) {
        super(REQUEST_ID);
        this.opId = opId;
        this.key = key;
    }

    public long getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "ReadRequest{" +
                "key=" + key +
                '}';
    }
}
