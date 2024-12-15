package protocols.abd.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

import java.nio.ByteBuffer;
import java.util.UUID;

public class WriteCompleteNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 114;

    private final UUID uuid;
    private final long key;
    private final long value;

    public WriteCompleteNotification(UUID id, long key, long value){
        super(NOTIFICATION_ID);
        this.uuid = id;
        this.key = key;
        this.value = value;
    }

    public UUID getOpId(){
        return uuid;
    }

    public byte[] getKey() {
        byte [] bytes = ByteBuffer.allocate(8).putLong(key).array();
        return bytes;
    }

    public long getValueLong(){
        return value;
    }

    public byte[] getValue() {
        byte [] bytes = ByteBuffer.allocate(8).putLong(value).array();
        return bytes;
    }

    @Override
    public String toString() {
        return "ReadCompleteNotification{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }






}
