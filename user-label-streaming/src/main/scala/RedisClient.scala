import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Created by ethan on 2017/11/16.
  */
object RedisClient {
  val redisHost = "localhost"
  val redisPort = 6379
  val redisTimeout = 30000

  val config = new JedisPoolConfig()
  config.setMaxTotal(500)
  config.setMaxIdle(30)
  config.setMinIdle(5)
  config.setMaxWaitMillis(1000 * 10)
  config.setTestWhileIdle(false)
  config.setTestOnBorrow(false)
  config.setTestOnReturn(false)

  val pool = new JedisPool(config, redisHost, redisPort, redisTimeout)
  println("init:" + pool)

  def getRedisPool(): Unit = {
    pool
  }
}
