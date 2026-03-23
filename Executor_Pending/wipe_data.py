from configparser import ConfigParser
import redis

config = ConfigParser()
config.read('config.ini')

rhost = config.get('infraParams', 'redisHost')
rport = config.getint('infraParams', 'redisPort')
rpass = config.get('infraParams', 'redisPass')
pending_stream = config.get('params','input_stream')
pending_queue = config.get('params','pending_queue')

stream_redis = redis.Redis(host=rhost, port=rport, password=rpass, db=12)
stream_redis.xtrim(pending_stream, maxlen=0, approximate=True)
stream_redis.delete(pending_queue)