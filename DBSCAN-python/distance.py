#!/usr/bin/env python
# encoding: utf-8
'''
@author: (╯°□°）╯︵┻━┻)
@contact: (╯°□°）╯︵┻━┻)@tencent.com
@file: distance.py
@time: 2019/5/29 12:00
@desc:
'''
import math


class MixedDistance(object):
    """docstring for MixedDistance"""
    def __init__(self, url_weight, uname_weight, coname_weight, city_weight, url_threshold=2, near=0, far=1):
        super(MixedDistance, self).__init__()
        self.STOP_ROOTS = set([ "h5.gdt.qq.com" ,
                                "m.dianping.com"  ,
                                "mp.weixin.qq.com",
                                "lp.pinduoduo.com",
                                "link.yiche.com",
                                "dmp-data.vip.com",
                                "aduland.dianping.com" ])
        self.FAR = far
        self.NEAR = near
        self.url_weight = url_weight
        self.uname_weight = uname_weight
        self.coname_weight = coname_weight
        self.url_threshold = url_threshold
        self.city_weight = city_weight


    def multiple_distance(self, a, b):
        ds =    self._hanming_dis(a[0], b[0]) + \
                self.url_weight * self._url_is_sim(a[1], b[1]) + \
                self.uname_weight * self._uname_is_sim(a[2], b[2]) + \
                self.coname_weight * self._coname_is_sim(a[3], b[3]) + \
                self.city_weight * self._jcd(a[4], b[4])
        return max(ds, 0)


    def hanming_distance(self, a, b):
        return self._hanming_dis(a[0], b[0])


    def skip_distance(self, a, b, txtweight=0):
        ds = 0
        if txtweight != 0:
            ds += self._hanming_dis(int(a[0]), int(b[0]))
        if self.url_weight != 0:
            ds += self.url_weight * self._url_is_sim(a[1], b[1])
        if self.uname_weight != 0:
            ds += self.uname_weight * self._uname_is_sim(a[2], b[2])
        if self.coname_weight != 0 :
            ds += self.coname_weight * self._coname_is_sim(a[3], b[3])
        if self.city_weight != 0 :
            ds += self.city_weight * self._jcd(a[4], b[4])
        return ds


    def _l1_dis(self, a, b):
        ret = 0
        for i,j in zip(a[0], b[0]):
            ret += abs(i - j)
        return ret / 12


    def _l2_dis(self, a, b):
        ret = 0
        for i,j in zip(a, b):
            ret += (i - j) ** 2
        return math.sqrt(ret)/128


    def _hanming_dis(self, a, b):
        x = bin(a^b)[2:]
        return x.count('1')/len(x)


    def _cos_dis(self, a, b):
        ret = 0
        mod_i = 0
        mod_j = 0
        for i,j in zip(a,b):
            ret += i*j
            mod_i += i**2
            mod_j += j**2
        return 1 - ret/(math.sqrt(mod_i) * math.sqrt(mod_j))


    def _url_distance(self, urla, urlb, scale=2):
        parsed_a = urla.split('/')[2:]
        parsed_b = urlb.split('/')[2:]
        if not parsed_b or not parsed_a or parsed_a[0] in self.STOP_ROOTS or parsed_b[0] in self.STOP_ROOTS:
            return 0 if scale == 2 else 1
        distance = 1
        cnt = 0
        for a,b in zip(parsed_a, parsed_b):
            if a == b:
                cnt += 1
            else:
                break
        return 1 - scale * ( cnt / min(len(parsed_a),len(parsed_b)) )


    def _url_is_sim(self, urla, urlb):
        if urla == urlb:
            return self.NEAR
        parsed_a = urla.split('/')[2:]
        parsed_b = urlb.split('/')[2:]
        if not parsed_b or not parsed_a:
            return 0
        if parsed_a[0] != parsed_b[0]:
            return self.FAR
        if parsed_a[0] in self.STOP_ROOTS:
            return 0
        cnt = self.url_threshold
        for a, b in zip(parsed_a, parsed_b):
            if a != b:
                cnt -= 1
                if cnt < 1:
                    return self.FAR
        return self.NEAR


    def _uname_is_sim(self, unamea, unameb):
        if unamea == "0":
            return 0
        return self.NEAR if unamea == unameb else self.FAR


    def _coname_is_sim(self, conamea, conameb):
        return self.NEAR if conamea == conameb else self.FAR


    def _jcd(self, a, b):
        if not a or not b:
            return 0
        x = set(a.split(","))
        y = set(b.split(","))
        # if x == y:
        #     return 0.5 * self.NEAR
        # else:
        #     if len(x) == len(y):
        #         return self.FAR
        #     else:
        #         return 0
        return 1 - ( len(x & y) / len(x | y) )



    # def ed_l1_dis(self, a, b, balance=0.3):
    #     ret = 0
    #     for i,j in zip(a[0], b[0]):
    #         ret += abs(i - j)
    #     # return min(ret/128, balance * url_distance(a[1], b[1]))
    #     ret = ret/128
    #     if url_distance(a[1], b[1]) < 0.3 and ret < 0.3:
    #         return -1
    #     return ret
    #     # return url_distance(a[1], b[1])

# MD = MixedDistance(1,1,1)
# from preprocess_by_simhash import *

def show(a,b):
    print("____"*10)
    x = jcd(a,b)
    print(x,end='  ,   ')
    emb = get_embedding(a)
    a = emb2int(emb, limit=0)
    emb = get_embedding(b)
    b = emb2int(emb, limit=0)
    y = MD._hanming_dis(a, b)
    print(y, "  ||  ", (x+y)/2)


if __name__ == '__main__':
    # import urllib.parse as urlparse
    # from nltk.metrics.distance import edit_distance

    a = [["q",1],["w",1],["e",1],["r",1],["t",1],["y",1],["u",1],["o",1]]
    b = [["z",1],["x",1],["c",1],["v",1],["b",1],["ggg",1],["m",1],["a",1]]
    show(a,b)

    print("add noise ..")
    a = [["q",1],["w",1],["e",1],["r",1],["t",1],["y",1],["u",1],["o",1]]
    b = [["z",1],["x",1],["c",1],["v",1],["b",1],["n",1],["m",1],["a",1],["t",1],["y",1],["u",1],["o",1]]
    show(a,b)

    a = [["q",1],["w",1],["e",1],["r",1],["t",0.1],["y",0.1],["u",0.1],["o",0.1]]
    b = [["z",1],["x",1],["c",1],["v",1],["b",1],["n",1],["m",1],["a",1],["t",0.1],["y",0.1],["u",0.1],["o",0.1]]
    show(a,b)

    print("move noise ..")
    a = [["q",1],["w",1],["e",1],["r",1]]
    b = [["z",1],["x",1],["c",1],["v",1],["b",1],["n",1],["m",1],["a",1]]
    show(a,b)

    a = [["q",1],["w",1],["e",1],["r",1]]
    b = [["q",1],["w",1],["c",1],["v",1],["b",1],["n",1],["m",1],["a",1]]
    show(a,b)

    a = [["q",1],["w",1],["e",1],["r",1]]
    b = [["q",1],["w",1],["c",1],["v",1],["b",1],["n",1]]
    show(a,b)


    print("best ..")
    a = [["q",1],["w",1],["e",1],["r",1]]
    b = [["q",1],["w",1],["e",1],["r",1],["b",1],["n",1],["m",1],["a",1]]
    show(a,b)

    print("test weight")
    a = [["q",0.1],["w",0.1],["e",0.999],["r",0.9]]
    b = [["q",0.1],["w",0.1],["c",0.99],["v",0.9],["b",0.1],["n",0.1]]
    show(a,b)

    a = [["q",0.5],["w",0.7],["e",0.999],["r",0.1]]
    b = [["q",0.5],["w",0.7],["c",0.9],["v",0.9],["b",0.9],["n",0.1]]
    show(a,b)

    a = [["q",0.5],["w",0.7],["e",0.999],["r",0.1]]
    b = [["q",0.5],["w",0.7],["c",0.1],["v",0.1],["b",0.1],["n",0.1]]
    show(a,b)

    print("old ..")
    a = [["q",1],["w",1],["e",1],["r",1],["t",1]]
    b = [["q",1],["w",1],["c",1],["v",1],["b",1]]
    show(a,b)

  
    a = [["q",1],["w",1],["e",1],["r",1],["t",1]]
    b = [["q",1],["w",1],["c",1]]
    show(a,b)

   
    a = [["q",1],["w",1],["e",1]]
    b = [["q",1],["w",1],["c",1]]
    show(a,b)

   
    a = [["q",1],["w",1],["e",1]]
    b = [["q",1],["w",1]]
    show(a,b)

   
    a = [["q",1],["w",1],["e",1]]
    b = [["q",1]]
    show(a,b)
