package com.zookeeper.dynamiconline;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @author XuBowen
 * @date 2021/10/27 21:08
 */
public class ZookeeperUtil {
	String connectString = "CentOS-02:2181";
	int sessionTimeout = 6000;
	ZooKeeper zooKeeper;



	// 初始化zk对象
	public ZooKeeper init() throws IOException {
		class MyWatcher implements Watcher {
			public void process(WatchedEvent event) {
				System.out.println(event.getPath()+"发生了 =>"+event.getType());
			}
		}

		zooKeeper = new ZooKeeper(connectString, sessionTimeout, new MyWatcher());
		return zooKeeper;
	}


}
