package com.zookeeper.dynamiconline;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author XuBowen
 * @date 2021/10/27 21:06
 */
public class Router {

	public static void main(String[] args) throws Exception {
		// 获取一个初始化zk对象
		ZooKeeper zooKeeper = new ZookeeperUtil().init();

		// 先启路由端
		Router router = new Router();
		router.check(zooKeeper);
		ArrayList<String> result = router.getData(zooKeeper);
		router.doOtherBusiness();
	}

	// 判断服务器根节点 /servers 是否存在，如果不存在则创建永久节点
	public void check(ZooKeeper zooKeeper) throws Exception{
		Stat stat = zooKeeper.exists("/servers", false);
		if (stat == null){
			zooKeeper.create("/servers","".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}

	// 获取zk集群进程，获取进程信息
	public ArrayList<String> getData(final ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
		ArrayList<String> resultSet = new ArrayList<String>();

		class GetDataWatch implements Watcher{
			public void process(WatchedEvent event) {
				System.out.println(event.getPath() + " 路径发生了 => " + event.getType());
				try {
					getData(zooKeeper);
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		List<String> children = zooKeeper.getChildren("/servers", new GetDataWatch());
		// 获取每个server节点的信息
		for (String child : children) {
			byte[] data = zooKeeper.getData("/servers/" + child, null, null);
			resultSet.add(new String(data));
		}
		System.out.println("最新读取到的信息是 => "+resultSet);
		return resultSet;

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
