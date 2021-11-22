import pymysql as ps
import pandas as pd
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

SQL = '''
select date, hour, count(distinct(imsi)) as user_num from
(
select date(start_time) as date, hour(start_time) as hour,imsi
from db_fact_psup_http_20211111
where cell in (select eci from temp_sport_cell_scene where site_name in ('JAP368', 'JAP423', 'JAP194')) group by date, hour, imsi
union all
select date(start_time) as date, hour(start_time) as hour,imsi
from db_fact_psup_https_20211111
where cell in (select eci from temp_sport_cell_scene where site_name in ('JAP368', 'JAP423', 'JAP194')) group by date, hour, imsi
union all
select date(start_time) as date, hour(start_time) as hour,imsi
from db_fact_psup_flow_20211111
where cell in (select eci from temp_sport_cell_scene where site_name in ('JAP368', 'JAP423', 'JAP194'))
group by date, hour, imsi
) as c group by date, hour

'''


def get_time_range(start_time, end_time):
    # 返回一个以天为粒度日期范围列表，参数start_time = '20210101' end_time = '20210201'
    time_range = pd.date_range(start=start_time, end=end_time, freq='D')

    date_list = [day.strftime('%Y%m%d') for day in time_range]

    return date_list


def replace_sql_date(SQL, day):
    # 将sql语句中的日期取代为day
    # day_sql = re.sub('(db_fact_\w*)(\d{8})', lambda x: x.group(1) + day, SQL)
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
        print('have fetched {} day data'.format(sum))
        sum += 1
        all_dataframe = all_dataframe.append(data)

    write_excel(all_dataframe)


def write_excel(dataframe):
    excel_path = os.path.join(os.path.expanduser("~"),
                              'Desktop') + '\\' + 'user_trend' + '.xlsx'

    with pd.ExcelWriter(excel_path, mode='w', engine='openpyxl') as writer:
        dataframe.to_excel(writer, index=False, sheet_name='kqi')
    print('----------data have writen into excel----------')


def install_package():
    os.system('pip install --upgrade pip')
    os.system('pip install numpy')
    os.system('pip install pandas')
    os.system('pip install openpyxl')


if __name__ == '__main__':
    install_package()
    # this is date list,input date range that you want to fetch data.
    date_list = get_time_range('20211113', '20211113')
    sql_list = get_sql_list(date_list)
    print('----------begin to fetch data----------')
    # 利用线程池执行sql语句，7代表同时开启7个线程并发取数据，可以自己定义
    threadpool_execute_data(sql_list, 4)
