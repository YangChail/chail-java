package org.chail;

import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.seatunnel.args.ServerCommandArgs;

public class Service {


    public static void main(String[] args) {
        ServerCommandArgs serverCommandArgs = new ServerCommandArgs();
        SeaTunnel.run(serverCommandArgs.buildCommand());
    }
}
