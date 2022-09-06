import json
import math


def is_all_zh(s):
    for c in s:
        if not ('\u4e00' <= c <= '\u9fa5'):
            return False
    return True


def idf(x, D):
    return math.log(D / x, 3e4)


def idf_from_json(json_file, output_flie):
    data = json.load(open(json_file))
    x = {}
    D = len(data)
    print("total docs:",D)

    nbnoises = 0
    for i in data:
        for word in i['wordlist']:
            if len(word) > 1 and is_all_zh(word):
                x[word] = x.get(word,0) + 1
            else:
                nbnoises += 1
    print("total noises:", nbnoises)
    print("total words:", len(x))
    print("max value:", max(x.values()))

    with open(output_flie,"w") as f:
        for word in x:
            value = idf(x[word], D)
            f.write(word+"\t"+str(value)+"\n")
    return


def idf_from_file(tf_file, output_flie):
    wd = {}
    nbnoises = 0
    with open(tf_file) as f:
        for line in f:
            word, tf = line[:-1].split("\t")
            if len(word) > 1 and is_all_zh(word):
                wd[word] = float(tf)
            else:
                nbnoises += 1
    print("total words", len(wd))
    print("removed words:", nbnoises)
    print("max value:", max(wd.values()))

    lc_d = {}
    with open("idf_fy_local.map") as f:
        for line in f:
            word, tf = line[:-1].split("\t")
            lc_d[word] = float(tf)

    with open(output_flie,"w") as f:
        for word in wd:
            value = idf(wd[word], 32711)
            if word in lc_d:
                value = lc_d[word]
            f.write(word+"\t"+str(value)+"\n")
    return


if __name__ == '__main__':
    tf_file = "simhash_fy_idf_keyword_top10_global_full_ad_uniq_url_fix_20190720_0806.json"
    output_flie = "fy_wc6000_0808.map"

    idf_from_json(tf_file, output_flie)

    
