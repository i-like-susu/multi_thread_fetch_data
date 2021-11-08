import pymysql as ps
import pandas as pd
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# 利用正则表达式进行日期的匹配，只需要将随便写个日期就可以，只要是连续的8个数字就行
SQL = '''
select date,dest_ip,
round(total_ul_traffic*8000/total_ul_duration/1024,0) 'ul thp(kbps)',
round(total_dl_traffic*8000/total_dl_duration/1024,0) 'dl thp(kbps)'
from(
select date(start_time) date,dest_ip,
sum(case when ul_usage_time>0 then ul_traffic else 0 end) total_ul_traffic,
sum(case when ul_usage_time>0 then ul_usage_time else 0 end) total_ul_duration,
sum(case when dl_usage_time>0 then dl_traffic else 0 end) total_dl_traffic,
sum(case when dl_usage_time>0 then dl_usage_time else 0 end) total_dl_duration
from db_fact_psup_flow_20210101 
where source_ip in (select ip from dim_enodebip_inner where city_id=910)
and service_id=13 and subservice_id=76923 
group by date,dest_ip
)a
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
    date_list = get_time_range('20211001', '20211108')
    sql_list = get_sql_list(date_list)
    print('----------开始取数据----------')
    # 利用线程池执行sql语句，7代表同时开启7个线程并发取数据，可以自己定义
    threadpool_execute_data(sql_list, 7)
