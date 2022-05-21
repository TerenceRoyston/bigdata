package com.zookeeper.dynamiconline;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @author XuBowen
 * @date 2021/10/27 21:02
 */
public class Server {

	String basePath="/servers";

	public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
		// 获取一个初始化zk对象
		ZooKeeper zooKeeper = new ZookeeperUtil().init();
		// 启动服务器端
		Server server = new Server();
		server.register("click",zooKeeper);
		Thread.sleep(5000);
		server.register("order",zooKeeper);
		Thread.sleep(5000);
		server.register("pay",zooKeeper);
		Thread.sleep(5000);
		server.doOtherBusiness();
	}

	// 使用zk客户端注册临时服务器节点
	public void register(String info,ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
		// 节点需要临时带序号
		zooKeeper.create(basePath+"/server",info.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
	}

	// 其它业务功能
	public void doOtherBusiness() throws InterruptedException {
		System.out.println("working......");
		// 进程不能停止
		while (true){
			Thread.sleep(5000);
			System.out.println("i am alive!!!");
		}
	}
}
