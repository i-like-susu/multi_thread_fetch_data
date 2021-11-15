import pymysql as ps
import pandas as pd
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# 利用正则表达式进行日期的匹配，只需要将随便写个日期就可以，只要是连续的8个数字就行
SQL = '''
select date,appname,dest_ip,sum(time) times,
round(sum(total_ul_rtt)/sum(total_ul_delay_cnt),0) as ul_avg_rtt,
round(sum(total_dl_rtt)/sum(total_dl_delay_cnt),0) as dl_avg_rtt,
round(sum(total_ul_loss_pkt)*100.0/sum(total_ul_pkt),2) ul_loss_ratio,
round(sum(total_dl_loss_pkt)*100.0/sum(total_dl_pkt),2) dl_loss_ratio,
round(sum(total_ul_retrans_pkt)*100.0/sum(total_ul_pkt),2) ul_retrans_ratio,
round(sum(total_dl_retrans_pkt)*100.0/sum(total_dl_pkt),2) dl_retrans_ratio,
round(sum(total_ul_traffic)*8000/sum(total_ul_duration)/1024,2) avg_ul_throughput,
round(sum(total_dl_traffic)*8000/sum(total_dl_duration)/1024,2) avg_dl_throughput,
round(sum(total_tcp_resp_delay)/sum(total_tcp_resp_cnt),2) as tcp_step12_latency,
round(sum(total_tcp_ack_delay)/sum(total_tcp_ack_cnt),2) as tcp_step23_latency,
round(sum(TCP12_suc)/sum(TCP12_req),2) TCP12_suc_rat,
round(sum(TCP23_suc)/sum(TCP23_req),2) TCP23_suc_rat
from (
select date(start_time) date,service_id||','||subservice_id id,dest_ip,count(*) time,
sum(ul_loss_pkt)  total_ul_loss_pkt,
sum(dl_loss_pkt)  total_dl_loss_pkt,
sum(ul_retrans_pkt)  total_ul_retrans_pkt,
sum(dl_retrans_pkt)  total_dl_retrans_pkt,
sum(ul_pkt) total_ul_pkt,
sum(dl_pkt) total_dl_pkt,
sum(ul_avg_rtt*ul_delay_cnt) as total_ul_rtt,
sum(ul_delay_cnt) as total_ul_delay_cnt,
sum(dl_avg_rtt*dl_delay_cnt) as total_dl_rtt,
sum(dl_delay_cnt) as total_dl_delay_cnt,
sum(tcp_resp_delay*tcp_resp_cnt) as total_tcp_resp_delay,
sum(tcp_resp_cnt) as total_tcp_resp_cnt,
sum(tcp_ack_delay*tcp_ack_cnt) as total_tcp_ack_delay,
sum(tcp_ack_cnt) as total_tcp_ack_cnt,
sum(case when ul_traffic>1024*1024 and ul_usage_time>0 then ul_traffic else 0 end) as total_ul_traffic,
sum(case when ul_traffic>1024*1024 and ul_usage_time>0 then ul_usage_time else 0 end) as total_ul_duration,
sum(case when dl_traffic>2*1024*1024 and dl_usage_time>0 then dl_traffic else 0 end) as total_dl_traffic,
sum(case when dl_traffic>2*1024*1024 and dl_usage_time>0 then dl_usage_time else 0 end) as total_dl_duration,
sum(case when l4protocol_id = 0  and synack_status in (0, 1) then 1 else 0 end) TCP12_req,
sum(case when l4protocol_id = 0  and synack_status = 0 then 1 else 0 end) TCP12_suc,
sum(case when l4protocol_id = 0 and ack_status in (0, 1) then 1 else 0 end) TCP23_req,
sum(case when l4protocol_id = 0  and ack_status = 0 then 1 else 0 end) TCP23_suc
from db_fact_psup_https_20211001 where l4protocol_id=0  and source_ip in (select ip from dim_enodebip_inner where city_id=910)
and dest_ip like '10.%'
group by date,id,dest_ip
)a  left join dim_service_new_ b on a.id=b.id
group by date,appname,dest_ip
'''


def get_time_range(start_time, end_time):
    # 返回一个以天为粒度日期范围列表，参数start_time = '20210101' end_time = '20210201'
    time_range = pd.date_range(start=start_time, end=end_time, freq='D')

    date_list = [day.strftime('%Y%m%d') for day in time_range]

    return date_list


def replace_sql_date(SQL, day):
    # 将sql语句中的日期取代为day
    day_sql = re.sub('(db_fact_\w*)(\d{8})', lambda x: x.group(1) + day, SQL)
    return day_sql


def get_sql_list(date_list):
    # 将sql中的日期替换成date_list中的日期，返回一个sql_list列表
    sql_list = [replace_sql_date(SQL, day) for day in date_list]
    return sql_list


def execute_sql(SQL):
    # 处理sql语句的函数,返回处理后的数据,以data_frame格式返回
    conn = ps.connect(host='10.245.146.71', port=5258, user='gbase', password='ZXvmax_2017',
                      database='zxvmax', charset='utf8mb4', autocommit=True)

    cursor = conn.cursor()

    cursor.execute(SQL)
    data = cursor.fetchall()

    # 获取列名
    col_tuple = cursor.description
    col_name = [col[0] for col in col_tuple]
    dataframe = pd.DataFrame(list(data), columns=col_name)
    return dataframe


def threadpool_execute_data(sql_list, thread_num):
    '''
            利用线程池来处理数据，提高数据提取的效率
            sql_list: 待处理的sql列表
            thread_num: 线程池中线程的个数
    '''
    executor = ThreadPoolExecutor(thread_num)
    sum = 1
    all_dataframe = pd.DataFrame()
    for data in executor.map(execute_sql, sql_list):
        print('已经取出{}天的数据'.format(sum))
        sum += 1
        all_dataframe = all_dataframe.append(data)

    write_excel(all_dataframe)


def write_excel(dataframe):
    excel_path = os.path.join(os.path.expanduser("~"),
                              'Desktop') + '\\' + 'day' + '.xlsx'

    with pd.ExcelWriter(excel_path, mode='w', engine='openpyxl') as writer:
        dataframe.to_excel(writer, index=False, sheet_name='kqi')
    print('----------数据成功写入excel----------')


if __name__ == '__main__':
    # 需要提取的日期，可以提取到开始时间和结束时间
    date_list = get_time_range('20211025', '20211111')
    sql_list = get_sql_list(date_list)
    print('----------开始取数据----------')
    # 利用线程池执行sql语句，7代表同时开启7个线程并发取数据，可以自己定义
    threadpool_execute_data(sql_list, 7)
