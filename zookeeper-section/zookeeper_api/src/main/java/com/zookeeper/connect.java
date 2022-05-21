package com.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author XuBowen
 * @date 2021/10/24 19:12
 */
public class connect {

	String connectString = "CentOS-02:2181";
	int sessionTimeout = 6000;
	ZooKeeper zooKeeper;

	@Before
	public void init() throws IOException {
		// 创建zookeeper对象

		// 重写回调方法，一旦watcher观察的path触发了指定的事件，服务端会通知客户端，客户端收到通知后会自动调用process()
		class MyWatcher implements Watcher {
			public void process(WatchedEvent event) {
				System.out.println(event.getPath()+"发生了 =>"+event.getType());
			}
		}

		zooKeeper = new ZooKeeper(connectString, sessionTimeout, new MyWatcher());
		System.out.println(zooKeeper);
	}

	@Test
	// 获取节点信息
	public void ls() throws KeeperException, InterruptedException {
		Stat stat = new Stat();
		List<String> children = zooKeeper.getChildren("/xubowen", null, stat);
		System.out.println(children);
		System.out.println(stat);
	}

	@Test
	// 创建节点
	public void create() throws KeeperException, InterruptedException {
		zooKeeper.create("/xubowen/python", "hello java".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	@Test
	// 获取节点信息
	public void get() throws KeeperException, InterruptedException {
		byte[] data = zooKeeper.getData("/xubowen", null, null);
		System.out.println(new String(data));
	}

	@Test
	// 节点赋值
	public void set() throws KeeperException, InterruptedException {
		zooKeeper.setData("/xubowen/scala", "yes java".getBytes(), -1);
	}

	@Test
	// 删除节点
	public void delete() throws KeeperException, InterruptedException {
		zooKeeper.delete("/xubowen/python", -1);
	}

	@Test
	// 递归删除
	public void rmr() throws KeeperException, InterruptedException {
		String path = "scala";
		List<String> children = zooKeeper.getChildren(path, false);
		for (String child : children) {
			zooKeeper.delete(path + "/" + child, -1);
		}
		zooKeeper.delete(path, -1);
	}

	@Test
	// 判断节点是否存在
	public void isExists() throws KeeperException, InterruptedException {
		Stat exists = zooKeeper.exists("/xubowen/scala", false);
		System.out.println(exists == null ? "不存在" : "存在");
	}

	@Test
	// ls设置观察者
	/*
	public void lsAndWatch() throws KeeperException, InterruptedException {
		// 传入参数true 设置默认的观察者，但是不建议，应该不同的方法去调用不同的观察者
		zooKeeper.getChildren("/xubowen",true);

		// 进程不能停止
		while (true){
			Thread.sleep(5000);
			System.out.println("i am alive!!!");
		}
	}
	*/

	public void lsAndWatch() throws Exception{
		class LsWatch implements Watcher{
			// 自定义回调逻辑
			public void process(WatchedEvent event) {
				System.out.println(event.getPath()+"发生了 =>"+event.getType());
				try {
					List<String> children = zooKeeper.getChildren("/xubowen", null);
					System.out.println(event.getPath() + "的新节点为 => " + children);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		zooKeeper.getChildren("/xubowen",new LsWatch());

		// 进程不能停止
		while (true){
			Thread.sleep(5000);
			System.out.println("i am alive!!!");
		}
	}

	private CountDownLatch count=new CountDownLatch(1);
	@Test
	// get设置观察者
	public void getWatch() throws Exception{

		class GetWatch implements Watcher{
			public void process(WatchedEvent event) {
				System.out.println(event.getPath() + " 路径发生了 => " + event.getType());
				try {
					byte[] data = zooKeeper.getData(event.getPath(), null, null);
					String res = new String(data);
					System.out.println(event.getPath()+" 节点修改后的值为 => "+res);
					count.countDown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		byte[] data = zooKeeper.getData("/xubowen/python", new GetWatch(), null);
		System.out.println("该节点当前值为 => "+new String(data));
		// 这是另外的阻塞线程的方法
		count.await();
	}


	@Test
	// 持续监听，主线程和监听线程要分开，监听线程不能阻塞
	public void mainLoop() throws InterruptedException, KeeperException {
		continuousListen();
		// 进程不能停止
		while (true){
			Thread.sleep(5000);
			System.out.println("i am alive!!!");
		}
	}

	@Test
	// 递归调用监听线程
	public void continuousListen() throws KeeperException, InterruptedException {
		class ContinuousLsWatch implements Watcher{
			// 自定义回调逻辑
			public void process(WatchedEvent event) {
				System.out.println(event.getPath()+"发生了 =>"+event.getType());
				try {
					List<String> children = zooKeeper.getChildren(event.getPath(), null);
					System.out.println(event.getPath() + "的新节点为 => " + children);
					continuousListen();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		zooKeeper.getChildren("/xubowen",new ContinuousLsWatch());

	}


	@After
	public void close() throws InterruptedException {
		if (zooKeeper != null) {
			zooKeeper.close();
		}
	}


}
