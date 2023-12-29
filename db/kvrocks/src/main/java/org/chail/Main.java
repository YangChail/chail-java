package org.chail;

import redis.clients.jedis.*;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;
import java.util.Set;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] args) {

        Jedis jedis = new Jedis("192.168.42.80", 6666);
        jedis.auth("mcadmin");
      /*  jedis.select(0);
        Set<String> keys = jedis.keys("*");
        jedis.set("aa", "value");
        String result = jedis.get("aa");*/
        NameSpaceCmd nameSpaceCmd = new NameSpaceCmd(jedis);
        String abc1 = nameSpaceCmd.del("abc1");
        abc1 = nameSpaceCmd.add("abc1");
        abc1 = nameSpaceCmd.get("abc1");
        List<String> all = nameSpaceCmd.getAll();

        System.out.println();
    }


    public void  addNameSpaceArgs(Jedis jedis,String name) {
        jedis.sendCommand(() -> SafeEncoder.encode("NAMESPACE"), new String[]{"add", name, name});
    }


    public static class NameSpaceCmd implements ProtocolCommand {
        private Jedis jedis;
        public NameSpaceCmd(Jedis jedis) {
            this.jedis = jedis;
        }
        protected CommandArguments commandArguments(ProtocolCommand command) {
            return new CommandArguments(command);
        }

        @Override
        public byte[] getRaw() {
            return SafeEncoder.encode("NAMESPACE");
        }


        public String add(String name){
            return jedis.getConnection().executeCommand(getString("add",name,name));
        }

        public String get(String name){
            return jedis.getConnection().executeCommand(getString("get",name));
        }


        public String del(String name){
            return jedis.getConnection().executeCommand(getString("del",name));
        }

        public List<String> getAll(){
            return jedis.getConnection().executeCommand(getStringList("get","*"));
        }


        private CommandObject<String> getString( String... arg) {
            return new CommandObject<>(commandArguments(this).keys( arg), BuilderFactory.STRING);
        }


        private CommandObject<List<String>> getStringList(String... arg) {
            return new CommandObject<>(commandArguments(this).keys( arg), BuilderFactory.STRING_LIST);
        }
    }


}