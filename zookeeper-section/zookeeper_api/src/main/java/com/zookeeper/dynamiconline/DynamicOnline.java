package com.zookeeper.dynamiconline;

/**
 * @author XuBowen
 * @date 2021/10/26 22:23
 */
public class DynamicOnline {
	public static void main(String[] args) throws Exception {
		// 此处不建议将server端与router端在一个线程中启动，因为先启一个线程的话程序会陷入循环中，无法进入下一个线程
		// 因此建议分开启动
	}


}
