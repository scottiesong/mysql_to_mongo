#!/usr/bin/python
# -*- coding: UTF-8 -*-

import time
from bson.objectid import ObjectId
import traceback
import datetime
from pymongo import MongoClient

if __name__ == '__main__':
    client = MongoClient('mongodb://10.0.70.31', 27017)
    db = client['admin']
    db.authenticate('nosqluser', '654321', mechanism='SCRAM-SHA-1')
    db = client['devel']

    # process start time
    start_time = datetime.datetime.now()

    step_length = 1000
    processed_count = 0
    loop_total_count = db.order_info_xiaozhang5.find({}).count()

    cal_total_count_time = datetime.datetime.now()

    print loop_total_count
    # print 'cal count time: ', (cal_total_count_time - start_time)

    skip_number = 2
    while (processed_count < loop_total_count):

        plank_order_info_rows = db.order_info_xiaozhang5.find({}).skip(skip_number).limit(step_length)

        for oi_row in plank_order_info_rows:
            row_start_time = datetime.datetime.now()
            ObjectId_oi = oi_row['_id']
            tvmid_oi = oi_row['tvmid']
            openid_oi = oi_row['openid']

            if tvmid_oi != '' and openid_oi != '':
                user_order_rows = db.user_order_xiaozhang.find({'tvmid': tvmid_oi, 'openid': openid_oi})
            elif tvmid_oi == '' and openid_oi != '':
                user_order_rows = db.user_order_xiaozhang.find({'openid': openid_oi})
            elif openid_oi == '' and tvmid_oi != '':
                user_order_rows = db.user_order_xiaozhang.find({'tvmid': tvmid_oi})
            else:
                # row_end_time = datetime.datetime.now()
                # print (row_end_time - row_start_time), processed_count
                continue

            utype_list = []
            grade_list = []
            into_shop_time_list = []
            last_shop_time_list = []
            loop_counter = 0

            for uor in user_order_rows:
                utype_list.append(uor['utype'])
                grade_list.append(uor['grade'])
                if uor['into_shop_time'] != 'None':
                    into_shop_time_list.append(uor['into_shop_time'])
                else:
                    into_shop_time_list.append('0001-01-01 00:00:00')

                if uor['last_shop_time'] != 'None':
                    last_shop_time_list.append(uor['last_shop_time'])
                else:
                    last_shop_time_list.append('0001-01-01 00:00:00')
                loop_counter += 1

            try:
                utype = int(max(utype_list))
                grade = int(max(grade_list))
                into_shop_time = max(into_shop_time_list)
                last_shop_time = max(last_shop_time_list)
            except BaseException, e:
                print 'str(Exception):\t', str(Exception)
                print 'str(e):\t\t', str(e)
                #print 'repr(e):\t', repr(e)
                print 'e.message:\t', e.message
                #print 'traceback.print_exc():', traceback.print_exc()
                print 'tvmid: ', tvmid_oi, 'openid: ', openid_oi
                print 'traceback.format_exc():\n%s' % traceback.format_exc()
                skip_number += 1
                continue

            if tvmid_oi != '' and openid_oi != '':
                order_count_per_user = db.order_info_xiaozhang5.find({'tvmid': tvmid_oi, 'openid': openid_oi})
            elif tvmid_oi == '' and openid_oi != '':
                order_count_per_user = db.order_info_xiaozhang5.find({'openid': openid_oi})
            elif openid_oi == '' and tvmid_oi != '':
                order_count_per_user = db.order_info_xiaozhang5.find({'tvmid': tvmid_oi})
            else:
                #order_count_per_user = 0
                continue

            order_num = 0

            mall_order = []
            Oid_list = []

            for order_rows in order_count_per_user:
                Oid_list.append(order_rows['_id'])
                nickname = order_rows['nickname']
                grand_total = float(order_rows['grand_total'])
                order_id = order_rows['order_id']
                quantity = int(order_rows['quantity'])
                entrance = order_rows['entrance']
                order_type = int(order_rows['order_type'])
                order_status = int(order_rows['order_status'])
                order_date = order_rows['order_date']
                product_id = order_rows['product_id']
                price = float(order_rows['price'])
                commission = float(order_rows['commission'])
                product_name = order_rows['product_name']
                order_fanli = float(order_rows['order_fanli'])

                mall_order.append({
                    'nickname': nickname,
                    'grand_total': grand_total,
                    'order_id': order_id,
                    'quantity': quantity,
                    'entrance': entrance,
                    'order_type': order_type,
                    'order_status': order_status,
                    'order_date': order_date,
                    'product_id': product_id,
                    'price': price,
                    'commission': commission,
                    'product_name': product_name,
                    'order_fanli': order_fanli
                })
                order_num += 1
            
            if order_num <= 0:
                continue

            timeA_last = time.strptime(last_shop_time, "%Y-%m-%d %H:%M:%S")
            ts_last = int(time.mktime(timeA_last))
            timeA_into = time.strptime(into_shop_time, "%Y-%m-%d %H:%M:%S")
            ts_into = int(time.mktime(timeA_into))

            mall_data = {
                'Order': mall_order,
                'order_num': order_num,
                'last_shop_time': ts_last,
                'into_shop_time': ts_into,
                'grade': grade,
                'utype': utype
            }

            if tvmid_oi != '':
                user_info_rows = db.user_info_xiangzhang5.find({'tvm_id': tvmid_oi})
                ui_row_count = user_info_rows.count()

                if ui_row_count > 0:
                    for ui_row in user_info_rows:
                        ObjectId_ui = ui_row['_id']
                        db.user_info_xiangzhang5.update({'_id': ObjectId(ObjectId_ui)}, {'$set': {'Mall': mall_data}})

                    for object_id in Oid_list:
                        db.order_info_xiaozhang5.remove({'_id': object_id})
                        processed_count += 1

                    # row_end_time = datetime.datetime.now()
                    # print (row_end_time - row_start_time), processed_count
                    # print '\t\t\t\t', {'Mall': mall_data}
                    continue

            if openid_oi != '':
                user_info_rows = db.user_info_xiangzhang5.find({'open_id': openid_oi})
                ui_row_count = user_info_rows.count()

                if ui_row_count > 0:
                    for ui_row in user_info_rows:
                        ObjectId_ui = ui_row['_id']
                        db.user_info_xiangzhang5.update({'_id': ObjectId(ObjectId_ui)}, {'$set': {'Mall': mall_data}})

                    for object_id in Oid_list:
                        db.order_info_xiaozhang5.remove({'_id': object_id})
                        processed_count += 1

                    # row_end_time = datetime.datetime.now()
                    # print (row_end_time - row_start_time), processed_count
                    # print '\t\t\t\t', {'Mall': mall_data}
                    continue

            # insert a new data
            insert_json = {
                'tvm_id': tvmid_oi,
                'open_id': openid_oi,
                'Mall': mall_data,
            }
            # print '\t\t\t\t', insert_json
            db.user_info_xiangzhang5.insert(insert_json)

            for object_id in Oid_list:
                db.order_info_xiaozhang5.remove({'_id': object_id})
                processed_count += 1
            # row_end_time = datetime.datetime.now()
            # print (row_end_time - row_start_time), processed_count

    end_time = datetime.datetime.now()

    print 'total time: ', (end_time - start_time)
