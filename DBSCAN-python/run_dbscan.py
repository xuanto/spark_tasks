#!/usr/bin/env python
# encoding: utf-8
'''
@author: (╯°□°）╯︵┻━┻)
@contact: (╯°□°）╯︵┻━┻)@tencent.com
@file: run_dbscan.py
@time: 2019/8/21 17:44
@desc:
'''

from dbscan import DBSCAN
from distance import MixedDistance
# import math
import json
import argparse
import numpy as np


def load_data(data_path, aid_idx, url_idx, feature_idx):
    total_data = []
    aid_list = []
    url_list = []
    with open(data_path) as f:
        for line in f:
            ll = line[:-1].split("\t")
            aid_list.append(ll[aid_idx])
            url_list.append(ll[url_idx])
            # feature = int(ll[feature_idx])
            feature = np.array([float(i) for i in ll[feature_idx].split(",")])
            total_data.append(feature)
    return aid_list , url_list , total_data


def main(args, distance):
    aid_list , url_list , data_list = load_data("textCNN/fy0822_aid_url_hashint_embedding_m0902_ep50ac97.data", 0, 1, 3)
    print("len data:", len(data_list))
    print("len array:", len(data_list[0]))
    dbscan = DBSCAN(    distance= distance,
                        eps = args.epsilon,
                        min_points = args.minpoints,
                        noise_as_clust = True,
                        start_cluster_id = 1,
                        start_nb_noise = 0,
                        show_pbar = args.show_pbar)
    tmp = []
    idx = np.random.randint(0, len(data_list))
    for i in range(len(data_list)):
        if i != idx:
            tmp.append(distance(data_list[idx], data_list[i]))
    tmp.sort()
    print(tmp[:20])
    print(tmp[::500])
    cluster_list, total_clusters, total_noises = dbscan.dbscan(data_list)
    print("total_noises:", total_noises)
    print("total_clusters:", total_clusters)
    with open(args.output, "w") as f:
        for i,j,k in zip(aid_list, url_list, cluster_list):
            f.write("\t".join([i,j,str(k)]) + "\n")
    return


def l2_distance(a,b):
    return np.linalg.norm(a - b)
    # return (2 * np.linalg.norm(a - b)) / (np.linalg.norm(a) + np.linalg.norm(b))
    # return 1 - (np.dot(a,b)/(np.linalg.norm(a)*(np.linalg.norm(b))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="hyper-parameters of run_cluster")
    parser.add_argument("--epsilon", "-e",help="epsilon", required=True, type=float)
    # parser.add_argument("--url_weight", "-u",help="url weight", default= 0, type=float) 
    # parser.add_argument("--uname_weight", "-n",help="user name weight", default= 0, type=float)
    # parser.add_argument("--coname_weight", "-c",help="cooperation name weight", default= 0, type=float)
    # parser.add_argument("--city_weight", "-l",help="city location weight", default= 0, type=float)
    # parser.add_argument("--start_cluster_id", "-s", help="start cluster id", default = 1, type=int)
    
    parser.add_argument("--minpoints", "-m", help="minimum points", default=3, type=float)
    # parser.add_argument("--datadir", "-d", help="hash file directory", default="fy_history_idf_keyword_top10_wc6000_0809.json")

    parser.add_argument("--show_pbar", "-v", help="show pbar", default=True)
    parser.add_argument("--output", "-o", help="output file name", default="fy_history_0808_e025")
    args = parser.parse_args()
    
    # distance = MixedDistance(   url_weight = args.url_weight, 
    #                             uname_weight = args.uname_weight, 
    #                             coname_weight = args.coname_weight, 
    #                             city_weight = args.city_weight,
    #                             near = -1)._hanming_dis  #.hanming_distance

    main(args, l2_distance)

