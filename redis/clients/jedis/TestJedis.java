/**
 * @Description:
 * @date:   2018年12月7日 下午12:13:20   
 * @version V1.0
 */
package redis.clients.jedis;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.RandomUtils;

/**
 * @Description: 
 * 
 * @author: 李光强
 * @date:   2018年12月7日 下午12:13:20
 */
public class TestJedis {

	public static void main(String[] args) {
		JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(200);
        poolConfig.setMaxIdle(10);
        poolConfig.setMaxWaitMillis(2000);
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestOnReturn(false);
        testSharedSentinel(poolConfig);

	}
	
	public static void testSharedSentinel(JedisPoolConfig poolConfig) {
		/**
		 * 服务器环境：两台主：master1和master2，分别带了两台从服务器。三台Sentinel服务器。
		 */
		Set<String> setSentinel = new HashSet<String>(); 
		setSentinel.add("134.192.34.247:26001"); 
		setSentinel.add("134.192.34.247:26002"); 
		setSentinel.add("134.192.34.247:26003");
		JedisSentinelPool jsp1 = new JedisSentinelPool("master1", setSentinel, poolConfig, "redis189");
		JedisSentinelPool jsp2 = new JedisSentinelPool("master2", setSentinel, poolConfig, "redis189");
		
		SentinelShareInfo shardInfo11 = new SentinelShareInfo("instance:1-1", jsp1);
		SentinelShareInfo shardInfo12 = new SentinelShareInfo("instance:1-2", jsp1);
		SentinelShareInfo shardInfo13 = new SentinelShareInfo("instance:1-3", jsp1);
		SentinelShareInfo shardInfo21 = new SentinelShareInfo("instance:2-1", jsp2);
		SentinelShareInfo shardInfo22 = new SentinelShareInfo("instance:2-2", jsp2);
		SentinelShareInfo shardInfo23 = new SentinelShareInfo("instance:2-3", jsp2);
		List<SentinelShareInfo> shards = Arrays.asList(shardInfo11, shardInfo12, shardInfo13, shardInfo21, shardInfo22,shardInfo23);
		
		ShardedSentinelPool pool = new ShardedSentinelPool(poolConfig, shards);
		for(int i=0; i<1000; i++) {
			ShardedSentinel ss = pool.getResource();
			//String key = "KEY" + RandomUtils.nextInt(10000, 100000000);
			//ss.set(key, String.valueOf(i));
			ss.set("key" + i, String.valueOf(RandomUtils.nextInt(10000, 100000000)));
			ss.close();
		}
		pool.close();
		System.out.println("testSharedSentinel结束.");
	}

}
