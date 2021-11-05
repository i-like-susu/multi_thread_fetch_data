import pymysql as ps
import pandas as pd
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

SQL = '''
select date,hour,dest_ip,sum(total_ul_delay_cnt+total_dl_delay_cnt) total_rtt_cnt,
round(sum(total_ul_rtt)/sum(total_ul_delay_cnt),0) as ul_avg_rtt,round(sum(total_dl_rtt)/sum(total_dl_delay_cnt),0) as dl_avg_rtt,
round(sum(total_ul_loss_pkt)*100.0/sum(total_ul_pkt),2) ul_loss_ratio,round(sum(total_dl_loss_pkt)*100.0/sum(total_dl_pkt),2) dl_loss_ratio,
round(sum(total_ul_retrans_pkt)*100.0/sum(total_ul_pkt),2) ul_retrans_ratio,round(sum(total_dl_retrans_pkt)*100.0/sum(total_dl_pkt),2) dl_retrans_ratio,
round(sum(total_ul_traffic)*8000/sum(total_ul_duration)/1024,2) avg_ul_throughput,
round(sum(total_dl_traffic)*8000/sum(total_dl_duration)/1024,2) avg_dl_throughput from (
select 
date(start_time) date,hour(start_time) hour,dest_ip,sum(ul_loss_pkt)  total_ul_loss_pkt,
sum(dl_loss_pkt)  total_dl_loss_pkt,
sum(ul_retrans_pkt)  total_ul_retrans_pkt,
sum(dl_retrans_pkt)  total_dl_retrans_pkt,
sum(ul_pkt) total_ul_pkt,
sum(dl_pkt) total_dl_pkt,
sum(ul_avg_rtt*ul_delay_cnt) as total_ul_rtt,
sum(ul_delay_cnt) as total_ul_delay_cnt,
sum(dl_avg_rtt*dl_delay_cnt) as total_dl_rtt,
sum(dl_delay_cnt) as total_dl_delay_cnt,
sum(case when ul_traffic>1024*1024 and ul_usage_time>0 then ul_traffic else 0 end) as total_ul_traffic,
sum(case when ul_traffic>1024*1024 and ul_usage_time>0 then ul_usage_time else 0 end) as total_ul_duration,
sum(case when dl_traffic>2*1024*1024 and dl_usage_time>0 then dl_traffic else 0 end) as total_dl_traffic,
sum(case when dl_traffic>2*1024*1024 and dl_usage_time>0 then dl_usage_time else 0 end) as total_dl_duration
from db_fact_psup_http_11111111 where l4protocol_id=0 and service_id=8
group by date,hour,dest_ip
union all
select 
date(start_time) date,hour(start_time) hour,dest_ip,sum(ul_loss_pkt)  total_ul_loss_pkt,
sum(dl_loss_pkt)  total_dl_loss_pkt,
sum(ul_retrans_pkt)  total_ul_retrans_pkt,
sum(dl_retrans_pkt)  total_dl_retrans_pkt,
sum(ul_pkt) total_ul_pkt,
sum(dl_pkt) total_dl_pkt,
sum(ul_avg_rtt*ul_delay_cnt) as total_ul_rtt,
sum(ul_delay_cnt) as total_ul_delay_cnt,
sum(dl_avg_rtt*dl_delay_cnt) as total_dl_rtt,
sum(dl_delay_cnt) as total_dl_delay_cnt,
sum(case when ul_traffic>1024*1024 and ul_usage_time>0 then ul_traffic else 0 end) as total_ul_traffic,
sum(case when ul_traffic>1024*1024 and ul_usage_time>0 then ul_usage_time else 0 end) as total_ul_duration,
sum(case when dl_traffic>2*1024*1024 and dl_usage_time>0 then dl_traffic else 0 end) as total_dl_traffic,
sum(case when dl_traffic>2*1024*1024 and dl_usage_time>0 then dl_usage_time else 0 end) as total_dl_duration
from db_fact_psup_https_11111111 where l4protocol_id=0 and service_id=8
group by date,hour,dest_ip
union all
select 
date(start_time) date,hour(start_time) hour,dest_ip,sum(ul_loss_pkt)  total_ul_loss_pkt,
sum(dl_loss_pkt)  total_dl_loss_pkt,
sum(ul_retrans_pkt)  total_ul_retrans_pkt,
sum(dl_retrans_pkt)  total_dl_retrans_pkt,
sum(ul_pkt) total_ul_pkt,
sum(dl_pkt) total_dl_pkt,
sum(ul_avg_rtt*ul_delay_cnt) as total_ul_rtt,
sum(ul_delay_cnt) as total_ul_delay_cnt,
sum(dl_avg_rtt*dl_delay_cnt) as total_dl_rtt,
sum(dl_delay_cnt) as total_dl_delay_cnt,
sum(case when ul_traffic>1024*1024 and ul_usage_time>0 then ul_traffic else 0 end) as total_ul_traffic,
sum(case when ul_traffic>1024*1024 and ul_usage_time>0 then ul_usage_time else 0 end) as total_ul_duration,
sum(case when dl_traffic>2*1024*1024 and dl_usage_time>0 then dl_traffic else 0 end) as total_dl_traffic,
sum(case when dl_traffic>2*1024*1024 and dl_usage_time>0 then dl_usage_time else 0 end) as total_dl_duration
from db_fact_psup_flow_11111111 where l4protocol_id=0 and service_id=8
group by date,hour,dest_ip
)a group by date,hour,dest_ip
'''


def get_time_range(start_time, end_time):
    # 返回一个以天为粒度日期范围列表，参数start_time = '20210101' end_time = '20210201'
    time_range = pd.date_range(start=start_time, end=end_time, freq='D')

    date_list = [day.strftime('%Y%m%d') for day in time_range]

    return date_list


def replace_sql_date(SQL, day):
    # 将sql语句中的日期取代为day
    day_sql = re.sub('\d{8}', day, SQL)
    return day_sql


def get_sql_list(date_list):
    # 将sql中的日期替换成date_list中的日期，返回一个sql_list列表
    sql_list = [replace_sql_date(SQL, day) for day in date_list]
    return sql_list


def execute_sql(SQL):
    # 处理sql语句的函数,返回处理后的数据,以data_frame格式返回
    conn = ps.connect(host='10.120.7.196', port=5258, user='gbase', password='ZXvmax_2017',
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
    date_list = get_time_range('20211001', '20211020')
    sql_list = get_sql_list(date_list)
    print('----------开始取数据----------')
    threadpool_execute_data(sql_list, 10)
