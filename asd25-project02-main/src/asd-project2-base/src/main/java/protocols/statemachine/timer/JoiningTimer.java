package protocols.statemachine.timer;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class JoiningTimer extends ProtoTimer {
    public static final short TIMER_ID = 100;

    public JoiningTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}

