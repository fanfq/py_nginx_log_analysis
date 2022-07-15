# 纯真网络，ip数据库
# https://github.com/animalize/qqwry-python3
# pip install qqwry-py3

# nginx日志分析
# https://cloud.tencent.com/developer/article/1622306

from qqwry import QQwry  # ip库
import re  # 正则
import datetime
import time
import pandas as pd  # 日志分析工具

# nginx日志文件
log_file_name = 'xxx.com.log'

qq = QQwry()
qq.load_file('qqwry.dat')
qq_version = qq.get_lastone()
print('qqwry ver.', qq_version)

obj = re.compile(
    r'(?P<ip>.*?)- - \[(?P<time>.*?)\] "(?P<request>.*?)" (?P<status>.*?) (?P<bytes>.*?) "(?P<referer>.*?)" "(?P<ua>.*?)"')


def timestamp_millis():
    return int(round(time.time() * 1000))


def log_load(path):
    start = timestamp_millis()
    lst = []
    with open(path, mode="r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            dic = parse(line)
            if dic:
                lst.append(dic)
            # print(rest)
        rows = len(lst)
        spend = timestamp_millis() - start
        print('log_load rows: {} ,spend: {} ms'.format(rows, spend))
        return lst


def ip_region(ipaddress):
    return qq.lookup(ipaddress)


def parse(line):
    # 解析单行nginx日志
    dic = {}
    try:
        result = obj.match(line)

        # ip处理
        ip = result.group("ip")
        if ip.strip() == '-' or ip.strip() == "":  # 如果是匹配到没有ip就把这条数据丢弃
            return False
        dic['ip'] = ip.split(",")[0]  # 如果有两个ip，取第一个ip

        ip_local = ip_region(dic['ip'])
        dic['ip_region'] = ip_local[0]
        dic['ip_network'] = ip_local[1]

        # 状态码处理
        status = result.group("status")  # 状态码
        dic['status'] = status

        # 时间处理
        req_time = result.group("time")  # 21/Dec/2019:21:45:31 +0800
        req_time = req_time.replace(" +0800", "")  # 替换+0800为空
        dist_time = datetime.datetime.strptime(req_time, "%d/%b/%Y:%H:%M:%S")  # 将时间格式化成友好的格式
        dic['time'] = str(dist_time)

        # request处理
        request = result.group(
            "request")  # GET /post/pou-xi-he-jie-jue-python-zhong-wang-luo-nian-bao-de-zheng-que-zi-shi/ HTTP/1.1
        a = request.split()[1].split("?")[0]  # 往往url后面会有一些参数，url和参数之间用?分隔，取出不带参数的url
        dic['request'] = a
        m = request.split()[0]
        dic['method'] = m

        # user_agent处理
        ua = result.group("ua")
        # if "Windows NT" in ua:
        #     u = "windows"
        # elif "iPad" in ua:
        #     u = "ipad"
        # elif "Android" in ua:
        #     u = "android"
        # elif "Macintosh" in ua:
        #     u = "mac"
        # elif "iPhone" in ua:
        #     u = "iphone"
        # else:
        #     u = "其他设备"
        dic['ua'] = ua

        # refer处理
        referer = result.group("referer")
        dic['referer'] = referer

        return dic
    except:
        pass


def time_format():
    # 时间处理
    req_time = '21/Dec/2019:21:45:31 +0800'
    req_time = req_time.replace(" +0800", "")  # 替换+0800为空
    dist_time = datetime.datetime.strptime(req_time, "%d/%b/%Y:%H:%M:%S")  # 将时间格式化成友好的格式
    print(dist_time)


def log_analysis(lst):
    df = pd.DataFrame(lst)
    print(df)

    # r = pd.value_counts(df['ip'])
    # print(r)

    # 自定义标题&20条返回
    ip_count = pd.value_counts(df['ip']).reset_index().rename(columns={"index": "ip", "ip": "count"}).iloc[:20, :]
    print(ip_count)

    # mutil
    r = pd.value_counts(df['ip_region']).reset_index().iloc[:30, :]
    print(r)


def log_write_to_json(lst):
    start = timestamp_millis()
    t = time.strftime("%Y%m%d", time.localtime())
    logs = open(log_file_name.replace('.log', '') + "." + t + ".json", "a+")
    logs.write(str(lst))
    logs.close()
    spend = timestamp_millis() - start
    print('log_write_to_json spend: {} ms'.format(spend))


def log_write_to_file(lst):
    start = timestamp_millis()
    t = time.strftime("%Y%m%d", time.localtime())
    logs = open(log_file_name.replace('.log', '') + "." + t + ".log", "a+")
    for l in lst:
        logs.write(str(l) + '\n')
    logs.close()
    spend = timestamp_millis() - start
    print('log_write_to_file spend: %{} ms'.format(spend))


if __name__ == '__main__':
    lst = log_load(log_file_name)
    # log_analysis(lst)
    log_write_to_json(lst)