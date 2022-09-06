#!/usr/bin/env python
# encoding: utf-8
'''
@author: (╯°□°）╯︵┻━┻)
@contact: (╯°□°）╯︵┻━┻)
@file: dbscan.py
@time: 2019/5/29 14:49
@desc:
'''
from tqdm import trange


class DBSCAN(object):
    """
    distance [function]: a function to compute the distance of two points.
    eps[int]: Maximum distance two points can be to be regionally related
    min_points[int]: The minimum number of points to make a cluster
    """
    def __init__(self, distance, eps, min_points, noise_as_clust=False, start_cluster_id=1, start_nb_noise=0, show_pbar=True):
        super(DBSCAN, self).__init__()
        self.distance = distance
        self.eps = eps
        self.min_points = min_points

        self.UNCLASSIFIED = False
        self.NOISE = 0
        self._noise_list = []
        self.noise_as_clust = noise_as_clust

        self.nb_noise = start_nb_noise
        self.cluster_id = start_cluster_id
        self.show_pbar = show_pbar
        self.min_delta = eps


    def _region_query(self, m, point_id):
        n_points = len(m)
        seeds = []
        min_dis_ = 10000
        nearst_neighbor = point_id
        for i in range(n_points):
            dis_ = self.distance(m[point_id], m[i])
            if dis_ < self.eps:
                seeds.append(i)
            if i != point_id and dis_ < min_dis_:
                min_dis_ = dis_
                nearst_neighbor = i
        return seeds, nearst_neighbor


    def _expand_cluster(self, m, classifications, point_id):
        seeds, nearst_neighbor = self._region_query(m, point_id)
        if len(seeds) < self.min_points:
            classifications[point_id] = self.NOISE
            self.nb_noise += 1
            self._noise_list.append([point_id, nearst_neighbor])
            return False
        else:
            classifications[point_id] = self.cluster_id
            # pbar.update(1)
            for seed_id in seeds:
                classifications[seed_id] = self.cluster_id
                # pbar.update(1)
            while len(seeds) > 0:
                current_point = seeds.pop(0)
                results, _ = self._region_query(m, current_point)
                if len(results) >= self.min_points:
                    for i, result_point in enumerate(results):
                        if classifications[result_point] == self.UNCLASSIFIED or classifications[result_point] == self.NOISE:
                            if classifications[result_point] == self.UNCLASSIFIED:
                                seeds.append(result_point)
                            classifications[result_point] = self.cluster_id
                            # pbar.update(1)
            return True


    def _parse_noise(self, m, classifications):
        for noise_ , neighbor_ in self._noise_list:
            if classifications[noise_] != self.NOISE:
                continue
            if (classifications[neighbor_] != self.NOISE) and (self.distance(m[noise_], m[neighbor_]) < self.min_delta):
                classifications[noise_] = classifications[neighbor_]
            else:
                classifications[noise_] = self.cluster_id
                self.cluster_id += 1
        return


    def _set_noise_as_uni(self, classifications):
        for noise_ , neighbor_ in self._noise_list:
            if classifications[noise_] != self.NOISE:
                continue
            else:
                classifications[noise_] = self.cluster_id
        self.cluster_id += 1
        return 


    def _load_history(self, file_path):
        history = []
        max_cid = 0
        with open(file_path) as f:
            for line in f:
                tmp = line[:-1].split("\t")
                hashid , cid = int(tmp[9]),int(tmp[5])
                history.append([hashid , cid])
                max_cid = max(max_cid, cid)
                # [ads["hash"], ads["url"], ads["uname"], ads["coname"], ads["city"]
        self.cluster_id = max_cid + 1
        return history


    def allocate_new_cluster_id(self, m, history_file_path):
        history = self._load_history(history_file_path)
        start_cluster_id = self.cluster_id
        print("start at cid:", start_cluster_id)
        n_points = len(m)
        classifications = [self.eps for i in range(n_points)]
        if self.show_pbar:
            iterator = trange(n_points)
        else:
            iterator = range(n_points)
        for point_id in iterator:
            nb_cid = -1
            for hist_data in history:
                dis_ = self.distance(m[point_id], hist_data)
                if dis_ < classifications[point_id]:
                    classifications[point_id] = dis_
                    nb_cid = hist_data[1]
            if nb_cid != -1:
                classifications[point_id] = nb_cid
            else:
                classifications[point_id] = self.cluster_id
                self.cluster_id += 1
            history.append([m[point_id][0], classifications[point_id]])
        return classifications, start_cluster_id, self.cluster_id


    def dbscan(self, m):
        """
        Inputs:
        m - A matrix whose columns are feature vectors
        
        Outputs:
        An array with either a cluster id number or dbscan.NOISE (None) for each
        column vector in m.
        """
        n_points = len(m)
        classifications = [self.UNCLASSIFIED for i in range(n_points)]
        if self.show_pbar:
            iterator = trange(n_points)
        else:
            iterator = range(n_points)
        for point_id in iterator:
            if classifications[point_id] == self.UNCLASSIFIED:
                if self._expand_cluster(m, classifications, point_id):
                    self.cluster_id += 1
        if self.noise_as_clust :
            self._parse_noise(m, classifications)
        else:
            self._set_noise_as_uni(classifications)
        self._noise_list = []
        return classifications, self.cluster_id, self.nb_noise

