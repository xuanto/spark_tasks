#!/usr/bin/env python
# encoding: utf-8
'''
@author: (╯°□°）╯︵┻━┻)
@contact: (╯°□°）╯︵┻━┻)@tencent.com
@time: 2019/8/15 18:52
@desc:
'''

import fasttext
from dbscan import DBSCAN
import numpy as np
import os
import argparse


def preprocess_data(data_path):
    tokens = []
    with open("/data/zadazhao/quality_product/fy_data/full_ad/fy_201907_08.token") as f:
        for line in f:
            wordlist = []
            for i in line[:-1].split(" "):
                tmp = i.split(",")
                if len(tmp) == 2:
                    word, weight = tmp
                    wordlist.append(word)
            tokens.append(" ".join(wordlist))

    labels = []
    aid = []
    with open("/data/zadazhao/quality_product/fy_data/full_ad/fy_full_ad_uniq_url_with_type_with_text_fix_title_20190720_0806.out") as f:
        for line in f:
            table = line[:-1].split("\t")
            labels.append(table[-5:-1])
            aid.append([table[1], table[6]])
    if len(labels) != len(tokens):
        print("error!!",len(labels),len(tokens))

    print(len(labels))
    cluster_data = []
    zcnt = 0
    zzcnt = 0
    zzzcnt = 0
    remove_cnt = 0
    with open(data_path,"w") as f:
        for i,j,k in zip(tokens, labels, aid):
            if j[args.industry_type] == "0" or j[args.industry_type][-2:] == "00" or not j[args.industry_type]:
                if j[args.industry_type] == "0":
                    zcnt += 1
                if j[args.industry_type][-2:] == "00":
                    zzcnt += 1
                if not j[args.industry_type]:
                    zzzcnt += 1
                if j[args.industry_type-1] == "0" or j[args.industry_type-1][-2:] == "00" or not j[args.industry_type-1]:
                    remove_cnt += 1
                else:
                    f.write(i + " __label__" + j[args.industry_type-1] + "\n")
                    cluster_data.append({"aid":k[0],"url":k[1],"wordlist":i})
                continue
            f.write(i + " __label__" + j[args.industry_type] + "\n")
            cluster_data.append({"aid":k[0],"url":k[1],"wordlist":i})
    print("number of 0 type: ", zcnt)
    print("number of 00 type: ", zzcnt)
    print("number of void type:", zzzcnt)
    print("number of removed type:", remove_cnt)
    return cluster_data


def train(data_path, model_path):
    model = fasttext.train_supervised(data_path, lr=0.2, dim=64, ws=3, epoch=200)
    model.save_model(model_path)
    return


def main(args, distance, total_data, start_cid, start_noise, output_file, show_pbar):
    #  
    #                             New history clusters
    #                 split by \t                                |  split by ','
    # aid, uid, uname, rho, delta, cluster_id, neighbor, wordlist,  url,  simhash_emb, city
    # 
    data_list = []
    h5_list = []
    e6_list = []
    
    for ads in total_data:
        data_list.append(ads["hash"])                
        h5_list.append('\t'.join([ads.get("aid","null"),ads.get("uid","null"),ads.get("uname","null"),'0','0']))
        # e6_list.append('\t'.join(['0', ','.join([w_w for w_w in ads.get("wordlist",[])]), ads.get("url","null"), str(ads["hash"]), ads.get("city","null")]))  
        e6_list.append('\t'.join(['0', ads.get("wordlist","?"), ads.get("url","null"), "null", ads.get("city","null")]))  

    if show_pbar:
        print("_____"*15)
        print("data list length: ", len(data_list), len(h5_list), len(e6_list))
    else:
        print("clusting:", ind_id, end="\r")
        # print("data list length: ", len(data_list), len(h5_list), len(e6_list),end="\r")

    # print("clusting ...")
    dbscan = DBSCAN(    distance= distance,
                        eps = args.epsilon,
                        min_points = args.minpoints,
                        noise_as_clust = True,
                        start_cluster_id = start_cid,
                        start_nb_noise = start_noise,
                        show_pbar = show_pbar )

    cluster_list, total_clusters, total_noises = dbscan.dbscan(data_list)
    print("total clusters:" , total_clusters)
    print("total nosie:", total_noises)
    
    for i, j, k in zip(h5_list, cluster_list, e6_list):
        output_file.write("\t".join([i, str(j), k]) + "\n")
    return total_clusters, total_noises


def cos_distance(a,b):
    return 1 - np.dot(a,b) / (np.linalg.norm(a) * np.linalg.norm(b))
    # return max(1 - np.dot(a,b) / 4096, 0)


def haming_distance(a,b):
    tmp = a*b
    tmp[tmp>=0] = 1
    tmp[tmp<0] = 0
    return tmp.sum() / tmp.size


def l1_distance(a,b):
    return np.abs(a-b) / a.size


def l2_distance(a,b):
    return np.sum((a - b) ** 2) / a.size


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="hyper-parameters of run_cluster")
    parser.add_argument("--epsilon", "-e",help="epsilon", required=True, type=float)
    parser.add_argument("--minpoints", "-m", help="minimum points", default=3, type=float)
    parser.add_argument("--show_pbar", "-v", help="show pbar", default=True)
    parser.add_argument("--output", "-o", help="output file name", default="fy_fft_0816_e025")
    parser.add_argument("--industry_type", "-t", help="id t", default=3, type=int)
    parser.add_argument("--distance_type", "-d", help="ds t", default="cos")
    args = parser.parse_args()

    model_path = "ftt_model_fy_20190720_tp%d_0820.bin" % args.industry_type
    data_path = "train_fy_20190720_tp%d.data" % args.industry_type
    cluster_data = preprocess_data(data_path)
    if not os.path.exists(model_path):
        print("training ...")
        train(data_path, model_path)
    else:
        print("loading ...")
    
    model = fasttext.load_model(model_path)
    print(model.test(data_path))

    for data in cluster_data:
        data["hash"] = model.get_sentence_vector(data["wordlist"])

    with open("fy_type"+str(args.industry_type+1)+"_aid_url_embedding_0820.data","w") as f:
        for data in cluster_data:
            tmp = ",".join([str(i) for i in data["hash"]])
            f.write("\t".join([data["aid"],data["url"],tmp]) + "\n")


    # output_file_name = "cluster_tp%d_%s_" % (args.industry_type, args.distance_type)
    # output_file_name = output_file_name + args.output

    # if args.distance_type == "cos":
    #     distance = cos_distance
    # elif args.distance_type == "l1":
    #     distance = l1_distance
    # elif args.distance_type == "l2":
    #     distance = l2_distance
    # elif args.distance_type == "hm":
    #     distance = haming_distance
    # with open(output_file_name, "a+", encoding='utf-8') as f:
    #     start_cid , start_noise = main(args, distance, cluster_data, 1, 0, f, args.show_pbar)
    #     print("total clusters:" , start_cid)
    #     print("total nosie:", start_noise)
    # print("all finished !!")
    # print("saved as:",output_file_name)