package protocols.statemachine.timer;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class HeartBeatTimer extends ProtoTimer {
    public static final short TIMER_ID = 101;

    public HeartBeatTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
