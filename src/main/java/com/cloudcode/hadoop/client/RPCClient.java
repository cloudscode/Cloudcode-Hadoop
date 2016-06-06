package com.cloudcode.hadoop.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;

import com.cloudcode.hadoop.Interface.IRPCInterface;

public class RPCClient {
public static void main(String[] args) throws Exception {
	ProtocolProxy<IRPCInterface> proxy =RPC.getProtocolProxy(IRPCInterface.class, 1, new InetSocketAddress("localhost",8888), new Configuration());
	String s = proxy.getProxy().test("client");
	System.out.println("client...>"+s);
}
}
