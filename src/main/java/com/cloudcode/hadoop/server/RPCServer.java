package com.cloudcode.hadoop.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import com.cloudcode.hadoop.Interface.IRPCInterface;


public class RPCServer implements IRPCInterface {
public static void main(String[] args) throws Exception {
	
	Server server =new RPC.Builder(new Configuration())
	.setBindAddress("localhost")
	.setPort(8888)
	.setInstance(new RPCServer())
	.setProtocol(IRPCInterface.class)
	.build();
	
	server.start();
}

public String test(String s) {
	System.out.println("RPCServer.test()");
	return "RPCServer...>"+s;
}


}
