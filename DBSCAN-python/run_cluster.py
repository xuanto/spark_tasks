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
import json
import argparse


def test(data_list, distance):
    print("here is some examples:")
    mark = len(data_list) // 4
    for j in [0,mark,mark*2,-mark,-1]:
        x = []
        mindis = 10086
        for i in data_list:
            ds = distance(data_list[j], i)
            x.append(ds)
            if ds < mindis :
                mindis = ds
                min_url = [data_list[j][1], i[1]]
        x.sort()
        print(x[:50:5])
        print("sum:",sum(x),"  len:",len(x),"  avg:",sum(x)/len(x))
        print(min_url)
        print("##### ######")
        print(x[0], x[mark],x[mark*2],x[-mark],x[-1])
        print("______"*12)
    print("")
    return


def debug(ida, idb, data_list, distance):
    tmp = []
    for data in data_list:
        if data['aid'] == ida: 
            a = data
        if data['aid'] == idb :
            b = data
    print(a['wordlist'])
    print(b['wordlist'])
    print(a['hash'], " || ", b['hash'])
    print("hanming_dis:", distance._hanming_dis(a['hash'], b['hash']))
    print(a['url'], " || ", b['url'])
    print("url:", distance._url_is_sim(a['url'], b['url']))
    print(a['uname'], " || ", b['uname'])
    print("uname:", distance._uname_is_sim(a['uname'], b['uname']))
    print(a['coname'], " || ", b['coname'])
    print("coname:", distance._coname_is_sim(a['coname'], b['coname']))
    print(a['city'], " || ", b['city'])
    print("city:", distance._jcd(a['city'], b['city']))


def main(args, distance, total_data, ind_id, idx, start_cid, start_noise, output_file, show_pbar):
    #  
    #                             New history clusters
    #                 split by \t                                |  split by ','
    # aid, uid, uname, rho, delta, cluster_id, neighbor, wordlist,  url,  simhash_emb, city
    # 
    data_list = []
    h5_list = []
    e6_list = []
    
    for ads in total_data:
        if args.industry_type == "all" or ads["industry_type"][idx] == ind_id:
            data_list.append([ads["hash"], ads.get("url","null"), ads.get("uname","null"), ads.get("coname","null"), ads.get("city","null")])                
            # data_list.append([ads["hash"], ads["url"], ads["industry_type"][idx], ads["coname"], ads["city"]]) 
            h5_list.append('\t'.join([ads.get("aid","null"),ads.get("uid","null"),ads.get("uname","null"),'0','0']))
            e6_list.append('\t'.join(['0', ','.join([w_w for w_w in ads.get("wordlist",[])]), ads.get("url","null"), str(ads["hash"]), ads.get("city","null")]))  

    # data_list = data_list[::2]
    # h5_list = h5_list[::2]
    # e6_list = e6_list[::2]
    
    if args.show_test:
        test(data_list, distance)

    if show_pbar:
        print("_____"*15)
        print("clusting: ", ind_id)
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
    if show_pbar:
        print("total clusters:" , total_clusters)
        print("total nosie:", total_noises)
    
    for i, j, k in zip(h5_list, cluster_list, e6_list):
        output_file.write("\t".join([i, str(j), k]) + "\n")
    return total_clusters, total_noises


def parse_zero(idx, total_data, ind_id):
    xdata = []
    for ads in total_data:
        # if True:
        # if (ads["industry_b"] == ind_id) or (ads["industry_a"] == ind_id):
        if ads["industry_type"][idx] == ind_id:
            xdata.append(ads)
    return xdata


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="hyper-parameters of run_cluster")
    parser.add_argument("--epsilon", "-e",help="epsilon", required=True, type=float)
    parser.add_argument("--url_weight", "-u",help="url weight", default= 0, type=float) 
    parser.add_argument("--uname_weight", "-n",help="user name weight", default= 0, type=float)
    parser.add_argument("--coname_weight", "-c",help="cooperation name weight", default= 0, type=float)
    parser.add_argument("--city_weight", "-l",help="city location weight", default= 0, type=float)
    parser.add_argument("--start_cluster_id", "-s", help="start cluster id", default = 1, type=int)
    
    parser.add_argument("--industry_type", "-i", help="industry type", default = 'all')
    # parser.add_argument("--industry_b", "-b", help="fine-grid industry type", default='n')
    parser.add_argument("--minpoints", "-m", help="minimum points", default=3, type=float)
    parser.add_argument("--url_threshold", "-t", help="url_threshold", default=2, type=int)
    parser.add_argument("--show_test", "-x", help="show test", default=False)
    # parser.add_argument("--datadir", "-d", help="hash file directory", default="tf_hash_list_total_ads_join_ind_corp_city.json")
    parser.add_argument("--datadir", "-d", help="hash file directory", default="fy_history_idf_keyword_top10_wc6000_0809.json")

    parser.add_argument("--show_pbar", "-v", help="show pbar", default=True)
    parser.add_argument("--output", "-o", help="output file name", default="fy_history_0808_e025")
    parser.add_argument("--debug", "-z", help="debug", default="")
    args = parser.parse_args()

    print("loading ...")
    if args.datadir[-5:] == ".json":
        with open(args.datadir, "r") as f:
            total_data = json.load(f)
    else:
        total_data = []
        with open(args.datadir, "r") as f:
            for line in f:
                aid, url, hashid = line[:-1].split("\t")
                if hashid == "0":
                    continue
                total_data.append({"aid":aid, "url":url, "hash":int(hashid)})

    distance = MixedDistance(   url_weight = args.url_weight, 
                                uname_weight = args.uname_weight, 
                                coname_weight = args.coname_weight, 
                                city_weight = args.city_weight,
                                near = -1,
                                # url_threshold = args.url_threshold).multiple_distance
                                url_threshold = args.url_threshold).hanming_distance

    if args.debug :
        ida, idb = args.debug.split('x')
        distance = MixedDistance(   url_weight = args.url_weight, 
                                    uname_weight = args.uname_weight, 
                                    coname_weight = args.coname_weight, 
                                    city_weight = args.city_weight,
                                    near = -1,
                                    url_threshold = args.url_threshold)
        debug(ida, idb, total_data, distance)
    else:
        with open("industry_ids.json") as f:
            industry_ids = json.load(f)
        # industry_ids = [args.industry_a]
        
        start_cid = args.start_cluster_id
        start_noise = 0
        output_file_name = "_".join(["cluster", args.industry_type, args.output])

        # idx = int(args.industry_type)        
        # with open(output_file_name, "a+", encoding='utf-8') as f:
        #     for ind_id in industry_ids[args.industry_type]:
        #         start_cid , start_noise = main(args, distance, total_data, ind_id, idx, start_cid, start_noise, f, args.show_pbar)
        #         if ind_id == '0':
        #             xdata = parse_zero(idx, total_data, ind_id)
        #             for ind_id_lv2 in industry_ids[str(idx-1)]:
        #                 start_cid , start_noise = main(args, distance, xdata, ind_id_lv2, idx-1, start_cid, start_noise, f, args.show_pbar)
        
        ind_id = args.industry_type
        idx = 3
        with open(output_file_name, "a+", encoding='utf-8') as f:
            start_cid , start_noise = main(args, distance, total_data, ind_id, idx, start_cid, start_noise, f, args.show_pbar)
        if not args.show_pbar:
            print("_____"*15)
            print("total clusters:" , start_cid)
            print("total nosie:", start_noise)
        print("all finished !!")
        print("saved as:",output_file_name)

