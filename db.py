#-*- coding:utf-8 -*-
from datetime import datetime
from datetime import timedelta
import os,sys
import MySQLdb
import MySQLdb.cursors as cors
import pandas as pd
import uuid
import traceback
import warnings
from feishu import FeiShu
import traceback
from log import logger
from utils import Utils


warnings.filterwarnings('ignore')


def get_year(start_date, end_date):
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
    month_dict = {}
    query_days = []
    while start_date_dt <= end_date_dt:
        date_str = start_date_dt.strftime("%Y-%m-%d")
        query_days.append(date_str)
        last_month = start_date_dt.month
        if last_month not in month_dict.keys():
            month_dict[last_month] = start_date_dt.year
        start_date_dt = start_date_dt + timedelta(days=1)    
    #month_dict[9] = 2023
    return month_dict, query_days

def insert_db(params):
    
    sql = "insert into yongyou_data values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    retry_times = 3
    while True:
        if retry_times < 0:
            raise Exception("入库失败，已重试3次")
        # conn = MySQLdb.connect("localhost", "root", "root", "bt_console", cursorclass = cors.DictCursor)
        conn = MySQLdb.connect("rm-2zetkwh4s22am33a0lo.mysql.rds.aliyuncs.com", "esznkj", "esznkj20231027@", "yongyou_data", cursorclass = cors.DictCursor)
        cur = conn.cursor()

        try:            
            # 批量插入  
            cur.executemany(sql, params)  
            conn.commit()
            break
        except Exception as e:
            retry_times -= 1
            logger.error("插入失败，" + traceback.format_exc())
            conn.rollback()
        finally:
            if cur:
                cur.close()

            if conn:
                conn.close()     

def remove_repeat_days_db(query_days):
    logger.info("开始删除重复数据...")
    for query_date in query_days:        
        date_parts = query_date.split("-")
        query_year = int(date_parts[0])
        query_month = int(date_parts[1])
        query_day = int(date_parts[2])

        retry_times = 3
        while True:
            if retry_times < 0:
                raise Exception("删除重复数据失败，已重试3次")
            # conn = MySQLdb.connect("localhost", "root", "root", "bt_console", cursorclass = cors.DictCursor)
            conn = MySQLdb.connect("rm-2zetkwh4s22am33a0lo.mysql.rds.aliyuncs.com", "esznkj", "esznkj20231027@", "yongyou_data", cursorclass = cors.DictCursor)
            cur = conn.cursor()
            query_sql = 'select * from yongyou_data where year = %s and month = %s and day = %s limit 1'%(query_year, query_month, query_day)
            sql = 'delete from yongyou_data where year = %s and month = %s and day = %s '%(query_year, query_month, query_day)

            try:     
                query_r = cur.execute(query_sql)
                if query_r == 0 or query_r is None:
                    break                 
                logger.info(f"正在删除重复数据：{query_date}...")       
                cur.execute(sql)  
                conn.commit()
                break
            except Exception as e:  
                retry_times -= 1
                logger.error("删除重复数据失败，" + traceback.format_exc())
                conn.rollback()
            finally:
                if cur:
                    cur.close()

                if conn:
                    conn.close() 

    logger.info("删除重复数据完成")

def main(data_folder, download_time, start_date, end_date):
    try:
        month_dict, query_days = get_year(start_date, end_date)
        remove_repeat_days_db(query_days)
        data_files = os.listdir(data_folder)
        Utils.safe_feishu(f"文件需要录入数量为 {len(data_files)}")
        seen = set()
        total_insert = 0
        total_skip = 0

        for data_file in data_files:
            f_file = os.path.join(data_folder, data_file)
            if 'part' not in data_file:
                df = pd.read_excel(f_file, skiprows=13, dtype=str)
                start_row_index = 16
            else:
                df = pd.read_excel(f_file, skiprows=1, dtype=str)
                start_row_index = 3
            batch = len(df) // 1000 + 1
            logger.info(f"文件: {f_file}, 开始分 {batch} 批次录入...")
            Utils.safe_feishu(f"文件: {data_file}, 开始分 {batch} 批次录入...")

            for index in range(batch):
                start_row = index * 1000
                end_row = min(index * 1000 + 1000, len(df))
                if end_row - start_row == 0:
                    break
                select_df = df.iloc[start_row:end_row]
                params = []
                skip_this_batch = 0
                for _, row in select_df.iterrows():
                    if pd.isnull(row.iloc[0]):
                        continue

                    main_account = "" if pd.isnull(row.iloc[2]) else str(row.iloc[2]).strip()
                    bill_date = "" if pd.isnull(row.iloc[4]) else str(row.iloc[4]).strip()
                    voucher_no = "" if pd.isnull(row.iloc[5]) else str(row.iloc[5]).strip()
                    entry_no = "" if pd.isnull(row.iloc[6]) else str(row.iloc[6]).strip()
                    summary = "" if pd.isnull(row.iloc[7]) else str(row.iloc[7]).strip()
                    subject_code = "" if pd.isnull(row.iloc[8]) else str(row.iloc[8]).strip()
                    subject_name = "" if pd.isnull(row.iloc[9]) else str(row.iloc[9]).strip()

                    debit_local  = 0.0 if pd.isnull(row.iloc[60]) else float(row.iloc[60])
                    credit_local = 0.0 if pd.isnull(row.iloc[63]) else float(row.iloc[63])
                    unique_key = (bill_date, voucher_no, entry_no, subject_code,
                                  debit_local, credit_local, main_account, summary)
                    if unique_key in seen:
                        skip_this_batch += 1
                        continue
                    seen.add(unique_key)

                    year, month, day = bill_date.split("-") if bill_date and "-" in bill_date else ("", "", "")

                    additional = "" if pd.isnull(row.iloc[53]) else str(row.iloc[53]).strip()
                    currency   = "" if pd.isnull(row.iloc[55]) else str(row.iloc[55]).strip()
                    debit_original  = 0.0 if pd.isnull(row.iloc[59]) else float(row.iloc[59])
                    credit_original = 0.0 if pd.isnull(row.iloc[62]) else float(row.iloc[62])
                    maker = "" if pd.isnull(row.iloc[65]) else str(row.iloc[65]).strip()
                    reviewer = "" if pd.isnull(row.iloc[66]) else str(row.iloc[66]).strip()
                    accounter = "" if pd.isnull(row.iloc[67]) else str(row.iloc[67]).strip()

                    file_dt = datetime.strptime(os.path.splitext(data_file)[0], "%Y%m%d%H%M%S")
                    create_date = file_dt.strftime("%Y%m%d")
                    download_time_dt = file_dt
                    params.append((
                        uuid.uuid4().hex, create_date, datetime.now(), download_time_dt, main_account,
                        year, month, day, voucher_no, entry_no, summary, subject_code, subject_name,
                        additional, currency, debit_original, debit_local, credit_original, credit_local,
                        "", "", "", "", maker, reviewer, accounter, ""
                    ))
                if params:
                    insert_db(params)
                    total_insert += len(params)
                total_skip += skip_this_batch

                logger.info(f"批次 {index+1}/{batch} 完成：插入 {len(params)}，跳过重复 {skip_this_batch}")

        # Utils.safe_feishue(f"录入数据库完成：插入 {total_insert} 条，跳过重复 {total_skip} 条")
        Utils.safe_feishu(f"录入数据库完成")

    except:
        Utils.safe_feishu(f"入库失败，{traceback.format_exc()}")


if __name__ == "__main__":
    
    # datafolder = sys.argv[1]
    # download_time = sys.argv[2]
    # start_date = sys.argv[3]
    # end_date = sys.argv[4]
    download_time = '20251126120000' # %Y%m%d%H%M%S
    datafolder = r"D:\qcyq\很久以前\Files\20251201134858"
    start_date = "2025-11-01"     
    end_date   = "2025-12-01"
    logger.info("开始入库...")
    main(datafolder, download_time, start_date, end_date)
    logger.info("入库完成")
