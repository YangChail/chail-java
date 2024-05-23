package org.chail;

import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class Client {


    public static void main(String[] args)
            throws FileNotFoundException, URISyntaxException, CommandException {
        //
        String property = System.getProperty("user.dir");
        if (System.getProperty("os.name").startsWith("Windows")) {
            System.setProperty("hadoop.home.dir", getTestConfigFile("/examples/hadoop"));
        }



        String configurePath = args.length > 0 ? args[0] : "/examples/fake_to_console.conf";
        String configFile = getTestConfigFile(configurePath);
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setConfigFile(configFile);
        clientCommandArgs.setCheckConfig(false);
        clientCommandArgs.setJobName(Paths.get(configFile).getFileName().toString());
        // Change Execution Mode to CLUSTER to use client mode, before do this, you should start
        // SeaTunnelEngineServerExample
        clientCommandArgs.setMasterType(MasterType.LOCAL);
        SeaTunnel.run(clientCommandArgs.buildCommand());
    }

    public static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = Client.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
