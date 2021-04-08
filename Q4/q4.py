#!/usr/bin/env python3
import sys, os
import argparse
import csv
import math
import re
from datetime import datetime, date

def main():
    try:
        # 1 コマンドライン引数を解析し, 入力ファイル名を決定する.  
        args = parseCommandLine()
        # 2 決定された入力ファイルを読み込み以下の形式で集計する.  
        data = parseCSV(args.log_file)
        # 3 2で集計されたデータ形式をIPごとに集計し直す.  
        data_byIP = accumerateByIP(data, args.m)

        # 4 3で得られた連想配列をもとに故障状態を求める.  
        failure_periods = detectFailurePeriods(data_byIP, args.N)
        # 5 故障状態を表示する
        printFailurePeriods(failure_periods, "IP")

        # 6 3で得られた連想配列をもとに過負荷期間を求める.  
        highload_periods = detectHighLoadPeriods(data_byIP, args.m, args.t)
        # 7 過負荷期間を表示する
        printHighLoadPeriods(highload_periods)

        # 8 データをネットワークアドレスごとに集計し直す
        data_bynetaddress = accumerateByNetAddress(data_byIP)
        # 9 ネットワークアドレス毎に故障期間を求める. 
        failure_periods_bynetaddress = detectFailurePeriods(data_bynetaddress, args.N)
        # 10 故障状態を表示する
        printFailurePeriods(failure_periods_bynetaddress, "NetAddress")

    except ValueError as e:
        print(e)


# 1 コマンドライン引数を解析し, 入力ファイル名を決定する.  
# argparseライブラリによって, コマンドライン引数を設定する
def parseCommandLine():
    parser = argparse.ArgumentParser()
    parser.add_argument("log_file", help="監視結果を保存したカンマ区切形式のログファイル")
    parser.add_argument("--N", type=int, default=1,\
            help="連続してタイムアウトしたときに故障とみなす回数")
    parser.add_argument("--m", type=int, default=1,\
            help="負荷判断するためのping回数")
    parser.add_argument("--t", type=int, default=4294967295,\
            help="負荷判断のしきい値時間[ms]")
    args = parser.parse_args()
    if(args.m < 1):
        raise ValueError("mは1以上の整数値を入力してください")
    if(args.t < 0):
        raise ValueError("tは0以上の整数値を入力してください")

    return args

# 2 決定された入力ファイルを読み込み以下の形式で集計する.  
# csvライブラリをつかって, CSVをパースする
# 要素が3以外の行は読み込まない. 
def parseCSV(filename):
    data_csv = []
    with open(filename, newline='') as fp:
        reader = csv.reader(fp, delimiter=',')
        for (i, data_line) in enumerate(reader):
            if(len(data_line) == 3):
                data_csv.append((i, data_line))
    return data_csv

# 3 2で集計されたデータ形式をIPごとに集計し直す.  
#
def accumerateByIP(data, m=1):
    data_byIP = dict()
    for (line, data_strs) in data:
        key = data_strs[1];
        date = data2datetime(data_strs[0], line)
        ping = data2ping(data_strs[2], line)
        if(key in data_byIP):
            data_byIP[key].append((date, ping))
        else:
            data_byIP[key] = [(date, ping)]
    for key,data in data_byIP.items():
        # logの順序が入れ替わっているときのために, ソートする. 
        data_byIP[key] = sorted(data_byIP[key], key=lambda x:x[0])

        # m回のpingの平均を求める
        appended_average = []
        for (i, (date, ping)) in enumerate(data):
            if(i < m-1):
                appended_average.append((date, ping, None))
                continue
            cnt = 0
            acc = 0
            for j in range(m):
                if(data[i-j][1] != None):
                    cnt += 1
                    acc += data[i-j][1]
            if(acc != 0):
                appended_average.append((date, ping, acc/cnt))
            else:
                appended_average.append((date, ping, None))
        data_byIP[key] = appended_average
    return data_byIP
    
# logfileのタイムスタンプをdatetimeに変換する
def data2datetime(date_str, line):
    try:
        return datetime.strptime(date_str,"%Y%m%d%H%M%S")
    except Exception as e:
        raise ValueError("タイムスタンプのフォーマットが不正 @line = " + str(line+1))

# logfileのpingをint/Noneに変換する
def data2ping(ping, line):
    val = None
    try:
        val = int(ping)
    except Exception as e:
        if(ping == None or ping != '-'):
            raise ValueError("ping値が不正 @line = " + str(line+1))
    return val

# IPごとに集計されたログデータから故障期間を求める
def detectFailurePeriods(data_ByIP, continuous_timeout):
    failures = dict()
    for key, info_list in data_ByIP.items():
        if(len(info_list) == 0):
            continue
        failures[key] = detectFailurePeriod(info_list, continuous_timeout)
    return failures

# 一つのIPに対して, 故障期間を計算する
def detectFailurePeriod(info_list, continuous_timeout=1):
    failures = []
    i = 0
    while(i < len(info_list)):
        (date, ping, ping_average) = info_list[i]
        if(ping == None):
            begin_i = i
            begin = date
            end = None
            while(i < len(info_list)):
                (date, ping, ping_average) = info_list[i]
                if(ping != None):
                    end = date
                    break
                i += 1
            if(i - begin_i >= continuous_timeout):
                failure = period_as_string(i - begin_i, begin, end)
                failures.append(failure)
        i += 1
    return failures


# 時間差分を文字列に変換する
def deltatime2str(delta):
    days    =  delta.days
    seconds =  delta.seconds%60
    mins    = (delta.seconds - seconds)//60%60
    hours   = (delta.seconds - seconds)//60//60
    ret = ""
    if(days != 0):
        ret = ret + "{}日".format(days)
    if(hours != 0):
        ret = ret + "{}時間".format(hours)
    if(mins != 0):
        ret = ret + "{}分".format(mins)
    if(seconds != 0):
        ret = ret + "{}秒".format(seconds)
    return ret + "間"

# 故障区間を表示する文字列に変換する. 
def period_as_string(n, begin, end):
    start_str = datetime.strftime(begin,"%Y年%m月%d日%H時%M分%S秒")
    if(end != None):
        end_str = datetime.strftime(end,"%Y年%m月%d日%H時%M分%S秒")
        diff = (end - begin)
        deltatime2str(diff)
        term = deltatime2str(diff)
    else:
        end_str = "継続中"
        term = ""
    return (n, start_str, end_str, term)


# 6 IPごとに集計されたログデータから過負荷期間を求める
def detectHighLoadPeriods(data_ByIP, m, t):
    highloads = dict()
    for key, info_list in data_ByIP.items():
        if(len(info_list) == 0):
            continue
        highloads[key] = detectHighLoadPeriod(info_list, m, t)
    return highloads

# 一つのIPに対して, 過負荷期間を計算する
def detectHighLoadPeriod(info_list, m, t):
    highloads = []
    i = 0
    while(i < len(info_list)):
        (date, ping, ping_average) = info_list[i]
        if(ping_average != None and ping_average >= t):
            begin_i = i
            begin = date
            end = None
            while(i < len(info_list)):
                (date, ping, ping_average) = info_list[i]
                if(ping_average != None  and ping_average < t):
                    end = date
                    break
                i += 1
            highload = period_as_string(i - begin_i, begin, end)
            highloads.append(highload)
        i += 1
    return highloads



# 5 故障区間の標準出力
def printFailurePeriods(failure_periods, groupName="IP"):
    print("{}\t\t故障はじめ\t\t\t故障終わり\t\t\t期間".format(groupName))
    for key, failures in failure_periods.items():
        if(len(failures) != 0):
            print(key)
        for f in failures:
            print("\t\t{}\t{}\t{}".format(f[1], f[2], f[3]))


# 7 過負荷期間の標準出力
def printHighLoadPeriods(highload_periods):
    if(len(highload_periods.items()) == 0):
        return
    print("IP\t\t過負荷はじめ\t\t\t過負荷終わり\t\t\t期間")
    for key, highloads in highload_periods.items():
        if(len(highloads) != 0):
            print(key)
        for f in highloads:
            print("\t\t{}\t{}\t{}".format(f[1], f[2], f[3]))

def accumerateByNetAddress(data_ByIP):
    data_by_netaddress = dict()
    for key, info_list in data_ByIP.items():
        netaddress = getNetAddressFromIP(key)
        if(netaddress not in data_by_netaddress):
            data_by_netaddress[netaddress] = []
        for l in data_ByIP[key]:
            data_by_netaddress[netaddress].append(l)

    for key in data_by_netaddress:
        # logの順序が入れ替わっているときのために, ソートする. 
        data_by_netaddress[key] = sorted(data_by_netaddress[key], key=lambda x:x[0])
    return data_by_netaddress

def getNetAddressFromIP(IP):
    match = re.match(r'(\d+)\.(\d+)\.(\d+)\.(\d+)/(\d+)$', IP)
    subnet_prefix = int(match.group(5))
    ip_as_int = \
            int(pow(256, 3)) * int(match.group(1)) + \
            int(pow(256, 2)) * int(match.group(2)) + \
            int(pow(256, 1)) * int(match.group(3)) + \
            int(pow(256, 0)) * int(match.group(4))
    mask = (int(math.pow(2, subnet_prefix)) - 1) * int(math.pow(2, 32-subnet_prefix))
    netaddress = mask & ip_as_int
    netaddress_A = netaddress // int(pow(256, 3)) % 256
    netaddress_B = netaddress // int(pow(256, 2)) % 256
    netaddress_C = netaddress // int(pow(256, 1)) % 256
    netaddress_D = netaddress // int(pow(256, 0)) % 256
    return "{}.{}.{}.{}".format(netaddress_A, netaddress_B, netaddress_C, netaddress_D)

if __name__ == "__main__":
    main()

