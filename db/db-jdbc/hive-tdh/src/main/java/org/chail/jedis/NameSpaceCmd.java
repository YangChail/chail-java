package org.chail.jedis;

import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.CommandObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;

/**
 * kvrocks 的namespace 的包装
 * yangc
 */
public  class NameSpaceCmd implements ProtocolCommand {
    public static final String  DEFAULT_IP="127.0.0.1";
    public static final Integer DEFAULT_PORT=40666;
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


        private CommandObject<String> getString(String... arg) {
            return new CommandObject<>(commandArguments(this).keys( arg), BuilderFactory.STRING);
        }


        private CommandObject<List<String>> getStringList(String... arg) {
            return new CommandObject<>(commandArguments(this).keys( arg), BuilderFactory.STRING_LIST);
        }
    }