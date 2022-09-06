#!/usr/bin/env python
# encoding: utf-8
'''
@author: (╯°□°）╯︵┻━┻)
@contact: (╯°□°）╯︵┻━┻)@tencent.com
@file: run_cluster.py
@time: 2019/6/3 14:44
@desc:
'''
from dbscan import DBSCAN
from distance import MixedDistance
# import math
# import json
# import argparse


def refine(cluster_file_name, output_file_name, distance, epsilon, minpoints, start_cid, start_noise, show_pbar):
    #  
    #                             New history clusters
    #                 split by \t                                |  split by ','
    # aid,  uid, uname , rho , delta,  cluster_id, neighbor, wordlist,  url, simhash_emb ,  city
    # 0  ,  1  ,    2  , 3   ,  4   ,       5    ,     6   ,      7  ,   8 ,       9     ,  10
    clusters = {}
    with open(cluster_file_name, "r") as f:
        print("loading ...")
        for line in f:
            data = line[:-1].split("\t")
            if data[5] not in clusters:
                clusters[data[5]] = {'data':[],'key':[]}
            clusters[data[5]]['data'].append(data)
            clusters[data[5]]['key'].append([data[9],data[8],data[1],data[2],data[10]])
            # clusters[data[5]]['key'].append(data[2])
            # clusters[data[5]]['e6'].append("\t".join(data[6:]))

    with open(output_file_name, "a+", encoding='utf-8') as f:
        print("clusting ", len(clusters), "clusters ...")
        for cid in clusters:
            cluster = clusters[cid]
            print("cid:",cid, "  nb_of_clusters:", len(cluster['key']), end=" | ")
            if len(cluster['key']) < 4 :
                print("skip!")
                continue    
            dbscan = DBSCAN(distance, epsilon, minpoints, True, start_cid, start_noise, show_pbar)
            cluster_list, total_clusters, total_noises = dbscan.dbscan(cluster['key'])
            for i, j in zip(cluster['data'], cluster_list):
                i[5] = i[5] + "_" + str(j)
                f.write("\t".join(i)+"\n")
            print("total_clusters:", total_clusters, "total_noises:", total_noises)
    return total_clusters, total_noises


def name_refine(cluster_file_name, output_file_name,epsilon, minpoints, start_cid, start_noise, show_pbar):
    #  
    #                             New history clusters
    #                 split by \t                                |  split by ','
    # aid,  uid, uname , rho , delta,  cluster_id, neighbor, wordlist,  url, simhash_emb ,  city
    # 0  ,  1  ,    2  , 3   ,  4   ,       5    ,     6   ,      7  ,   8 ,       9     ,  10
    cnames = {}
    cid = 1
    with open(output_file_name, "a+", encoding='utf-8') as wf:
        with open(cluster_file_name, "r") as f:
            print("loading ...")
            for line in f:
                data = line[:-1].split("\t")
                if data[2] not in cnames:
                    cnames[data[2]]= str(cid)
                    cid += 1
                data[5] = data[5] + "_" + cnames[data[2]]
                wf.write("\t".join(data) + "\n")
    print("name_refine finished !!")
    print("nb conames:", len(cnames), "clusters:", cid)
    return


if __name__ == '__main__':
    # name_refine(    cluster_file_name="cluster_all_ib_icc_0724_2441_e05",
    #                 output_file_name="cluster_all_ib_icc_0724_2441_e05_name.refine",
    #                 epsilon=0.1,
    #                 minpoints=3,
    #                 start_cid=1,
    #                 start_noise=0,
    #                 show_pbar=False )

    distance = MixedDistance(   url_weight = 0, 
                                uname_weight = 1,
                                coname_weight = 1, 
                                city_weight = 0,
                                near = 0,
                                url_threshold = 2 ).skip_dis
    refine( cluster_file_name="cluster_ia_5_cid1to354_0729_txt_url_e03",
            output_file_name="cluster_ia_5_0730_txt_url_uid_uname_e03.refine",
            distance=distance,
            epsilon=0.15,
            minpoints=2,
            start_cid=1,
            start_noise=0,
            show_pbar=False )

    # distance = MixedDistance(   url_weight = 1, 
    #                             uname_weight = 1,
    #                             coname_weight = 1, 
    #                             city_weight = 0,
    #                             near = 0,
    #                             url_threshold = 2 ).skip_dis
    # refine( cluster_file_name="cluster_ia_5_cid1to1598_onlytxt_e025",
    #         output_file_name="cluster_ia_5_1598_e15_uuc.refine",
    #         distance=distance,
    #         epsilon= 1.5,
    #         minpoints=3,
    #         start_cid=1,
    #         start_noise=0,
    #         show_pbar=False )
        
