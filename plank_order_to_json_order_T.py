#!/usr/bin/python
# -*- coding: UTF-8 -*-

import time
import datetime
from pymongo import MongoClient

if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db = client['admin']
    db.authenticate('nosqluser', '654321', mechanism='SCRAM-SHA-1')
    db = client['devel']

    rows_count = 0
    step_length = 1000
    total_processed_person_count_in_this_time = 0
    total_delete_rows_in_this_time = 0

    max_order_one_person = 0
    # process start time
    start_time = datetime.datetime.now()

    while (1):

        # # tvmid != '' && openid != ''
        # plank_order_info_rows = db.order_info_xiaozhang4.find({'tvmid': {'$ne': ''}, 'openid': {'$ne': ''}}).limit(step_length)
        # tvmid != '' && openid == ''
        plank_order_info_rows = db.order_info_xiaozhang4.find({'tvmid': {'$ne': ''}, 'openid': ''}).limit(step_length)
        # # tvmid == '' && openid != ''
        # plank_order_info_rows = db.order_info_xiaozhang4.find({'tvmid': '', 'openid': {'$ne': ''}}).limit(step_length)

        rows_count = plank_order_info_rows.count()
        # print 'total count: ', rows_count
        if rows_count <= 0:
            exit(1)

        for rows in plank_order_info_rows:
            ObjectId = rows['_id']
            tvmid = rows['tvmid']
            openid = rows['openid']
            # print '\t', ObjectId, tvmid, 'o: ', openid

            # # tvmid != '' && openid != ''
            # get_user_order_rows = db.user_order_xiaozhang2.find({'$or': [{'tvmid': tvmid}, {'openid': openid}]})
            # tvmid != ''
            get_user_order_rows = db.user_order_xiaozhang2.find({'tvmid': tvmid})

            # print 'user_order rows count: ', get_user_order_rows.count()

            utype_list = []
            grade_list = []
            into_shop_time_list = []
            last_shop_time_list = []
            loop_counter = 0

            for uor in get_user_order_rows:
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
                # print '\t', 'user_order debug info', '[', loop_counter, ']: ', uor['utype'], '| ', uor['grade'], '| ', uor[
                #     'into_shop_time'], '| ', uor['last_shop_time']
                loop_counter += 1

            utype = int(max(utype_list))
            grade = int(max(grade_list))
            into_shop_time = max(into_shop_time_list)
            last_shop_time = max(last_shop_time_list)
            # print '\t', 'user_order: ', utype, '| ', grade, '| ', into_shop_time, '| ', last_shop_time

            # # tvmid != '' && openid != ''
            # get_order_info_rows = db.order_info_xiaozhang4.find({'$or': [{'tvmid': tvmid}, {'openid': openid}]})
            # tvmid != ''
            get_order_info_rows = db.order_info_xiaozhang4.find({'tvmid': tvmid})
            # # openid != ''
            # get_order_info_rows = db.order_info_xiaozhang4.find({'openid': openid})

            order_num = get_order_info_rows.count()
            # print '\t', 'order num: ', order_num

            if order_num == 0:
                continue

            if order_num > max_order_one_person:
                max_order_one_person = order_num

            mall_order = []
            Oid_list = []

            for order_rows in get_order_info_rows:
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

            timeA_last = time.strptime(last_shop_time, "%Y-%m-%d %H:%M:%S")
            ts_last = int(time.mktime(timeA_last))
            timeA_into = time.strptime(into_shop_time, "%Y-%m-%d %H:%M:%S")
            ts_into = int(time.mktime(timeA_into))

            insert_json = {
                'last_login': ts_into,
                'tvm_id': tvmid,
                'open_id': '',
                'Mall': {
                    'Order': mall_order,
                    'order_num': order_num,
                    'last_shop_time': ts_last,
                    'into_shop_time': ts_into,
                    'grade': grade,
                    'utype': utype
                },
            }
            # print insert_json

            # insert to a new records
            db.order_info_final_1.insert(insert_json)

            total_processed_person_count_in_this_time += 1

            # delete had merged record of order_info
            delete_count = 0
            for object_id in Oid_list:
                # print object_id
                db.order_info_xiaozhang4.remove({'_id': object_id})
                delete_count += 1
            # print '\t', 'delete count: ', delete_count
            total_delete_rows_in_this_time += delete_count
            # print '======================================================================================='

    print 'total delete rows in this time: ', total_delete_rows_in_this_time
    print 'max orders one person in this time: ', max_order_one_person
    print 'total processed person count in this time: ', total_processed_person_count_in_this_time

    # process end time
    end_time = datetime.datetime.now()
    print 'total process seconds: ', (end_time - start_time).seconds
