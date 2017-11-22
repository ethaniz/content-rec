# -*- coding:utf-8 -*-

import os
from gensim.models import Word2Vec
import pymysql
import pandas as pd
from gensim import matutils
import numpy as np
from sklearn.metrics.pairwise import linear_kernel
import redis
import logging

model = Word2Vec.load(os.path.join('/Users/ethan/MyCodes/nlp', 
                                    'embedding_model_t2s/zhwiki_embedding_t2s.model'))

def label_clean(x):
    """
    标签按逗号分割，并去除无标签歌曲
    """
    global model
    ret = [label for label in x.split(',') if label in model.wv.vocab]
    return (ret if len(ret) != 0 else None)

def get_vector(list):
    """
    得出一个词list的vector
    仿照gensim的n_similary实现
    """
    global model
    v = [model.wv[word] for word in list]
    nparray = np.array(v).mean(axis=0)
    return matutils.unitvec(nparray)

def getAndCleanSongLabel():
    """
    从原始数据库中得出歌曲标签信息并清洗
    """
    conn = pymysql.connect(host='localhost',
                            port=3306,user='root',
                            passwd='baojing520',
                            charset='UTF8',
                            db='angel_stone')

    #cur = conn.cursor()
    sql = "select id, song_name, song_label from song where song_label is not null"
    df = pd.read_sql(sql, conn)
    conn.close()

    df['clean'] = df['song_label'].apply(label_clean)

    df_clean = df.dropna().reset_index()

    df_clean['vector'] = df_clean['clean'].apply(get_vector)

    return df_clean

def makeSimilaryMatrix(df):
    """
    构建歌曲相似度矩阵
    """
    cosine = linear_kernel(np.array(df['vector'].tolist()), 
                            np.array(df['vector'].tolist()))
    return cosine

def matrixToRedis(df, mat):
    """
    构建每一个物品的相似度列表，存入redis
    """
    REDIS_URL = 'redis://localhost:6379'
    _r = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
    SIMKEY = 'p:smlr:%s'
    for idx, row in df.iterrows():
        similar_indices = mat[idx].argsort()[:-100:-1]
        list = [(mat[idx][i], df['id'][i]) for i in similar_indices]
        flattened = sum(list[1:], ())
        _r.zadd(SIMKEY % row['id'], *flattened)

def main():
    logging.info("Start to clean label...")
    df_clean = getAndCleanSongLabel()
    logging.info("Start to make matrix...")
    cosine = makeSimilaryMatrix(df_clean)
    logging.info("Start to flush redis...")
    matrixToRedis(df_clean, cosine)
    logging.info("Done")

if __name__ == '__main__':
    main()