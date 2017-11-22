# -*- coding:utf-8 -*-
import pymysql
import redis
import pandas as pd
import time

def get_item_name(item_id):
    """
    从原始数据库中得出歌曲名
    """
    conn = pymysql.connect(host='localhost',
                            port=3306,user='root',
                            passwd='baojing520',
                            charset='UTF8',
                            db='angel_stone')

    cur = conn.cursor()
    sql = "select song_name from song where id=%s" % item_id
    #print(sql)
    cur.execute(sql)
    ret = cur.fetchone()
    conn.close()
    return ret[0]

def get_similary(item_id, redisClient):
    print(get_item_name(item_id))
    SIMKEY = 'p:smlr:%s'
    similay_list = redisClient.zrange(SIMKEY % item_id, 0, 10-1, withscores=True, desc=True)
    return similay_list


def get_user_history(user_id, _r):
    time1 = time.time()
    r = redis.Redis(host='localhost', port=6379, db=5)
    di = r.hgetall(user_id)
    #print(type(di))
    dfs = []
    time2 = time.time()
    for item_id, weight in di.items():
        #print(item_id.decode(), weight.decode())
        df = pd.DataFrame(get_similary(item_id.decode(), _r), columns=['item', 'score'])
        df['score'] = df['score'] * float(weight.decode())
        dfs.append(df)
    time3 = time.time()
    df_final = pd.concat(dfs).groupby('item').sum().sort_values(by=['score'], ascending=False)[:30]
    time4 = time.time()
    for index, row in df_final.iterrows():
        #print(get_item_name(index.decode()), row['score'])
        print(index.decode(), row['score'])
    time5 = time.time()

    print("time5-time4 : %f" % (time5-time4))
    print("time4-time3 : %f" % (time4-time3))
    print("time3-time2 : %f" % (time3-time2))
    print("time2-time1 : %f" % (time2-time1))

def main():
    REDIS_URL = 'redis://localhost:6379'
    _r = redis.StrictRedis.from_url(REDIS_URL)
    #list = get_similary('100', _r)
    #for id, score in list:
    #    print(get_item_name(id.decode()), score)
    get_user_history('135', _r)


if __name__ == '__main__':
    main()