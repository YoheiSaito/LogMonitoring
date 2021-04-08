#!/usr/bin/env python3
import sys, os
import argparse
import csv
import math
from datetime import datetime, date

def main():
    try:
        # 1 コマンドライン引数を解析し, 入力ファイル名を決定する.  
        args = parseCommandLine()
        # 2 決定された入力ファイルを読み込み以下の形式で集計する.  
        data = parseCSV(args.log_file)
        # 3 2で集計されたデータ形式をIPごとに集計し直す.  
        data_byIP = accumerateByIP(data)
        # 4 3で得られた連想配列をもとに故障状態を求める.  
        failure_periods = detectFailurePeriods(data_byIP)
        # 5 故障状態を表示する
        printFailurePeriods(failure_periods)
    except ValueError as e:
        print(e)


# 1 コマンドライン引数を解析し, 入力ファイル名を決定する.  
# argparseライブラリによって, コマンドライン引数を設定する
def parseCommandLine():
    parser = argparse.ArgumentParser()
    parser.add_argument("log_file", help="監視結果を保存したカンマ区切形式のログファイル")
    return parser.parse_args()

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
def accumerateByIP(data):
    data_byIP = dict()
    for (line, data_strs) in data:
        key = data_strs[1];
        date = data2datetime(data_strs[0], line)
        ping = data2ping(data_strs[2], line)
        if(key in data_byIP):
            data_byIP[key].append((date, ping))
        else:
            data_byIP[key] = [(date, ping)]
    for key in data_byIP:
        data_byIP[key] = sorted(data_byIP[key], key=lambda x:x[0])
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
def detectFailurePeriods(data_ByIP):
    failures = dict()
    for key, info_list in data_ByIP.items():
        if(len(info_list) == 0):
            continue
        failures[key] = detectFailurePeriod(info_list)
    return failures

# 一つのIPに対して, 故障期間を計算する
def detectFailurePeriod(info_list):
    failures = []
    i = 0
    while(i < len(info_list)):
        (date, ping) = info_list[i]
        if(ping == None):
            begin_i = i
            begin = date
            end = None
            while(i < len(info_list)):
                (date, ping) = info_list[i]
                if(ping != None):
                    end = date
                    break
                i += 1
            failure = failure_as_string(i - begin_i, begin, end)
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
def failure_as_string(n, begin, end):
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

# 故障区間の標準出力
def printFailurePeriods(failure_periods):
    print("IP\t\t故障はじめ\t\t\t故障終わり\t\t\t期間")
    for key, failures in failure_periods.items():
        if(len(failures) != 0):
            print(key)
        for f in failures:
            print("\t\t{}\t{}\t{}".format(f[1], f[2], f[3]))

if __name__ == "__main__":
    main()
        
