#!/usr/bin/python
# -*- coding: UTF-8 -*-

import time
import datetime
import traceback
from pymongo import MongoClient

if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db = client['admin']
    db.authenticate('nosqluser', '654321', mechanism='SCRAM-SHA-1')
    db = client['devel2']

    plank_order_info_rows = 0
    user_order_rows = 0
    order_count_per_user = 0
    user_info_rows = 0

    id_list = {}  # []
    file_write_list = {}

    each_id = ''
    open_id = ''
    loop_count = 0
    # step_1_loop_count = 0

    ui_new_row = ''
    total_count = 0

    skip_number = 0

    start_time = datetime.datetime.now()
    object_id = ''
    tvmid_ui = ''
    openid_ui = ''

    output = open('data', 'w')
    f = open('process_log.log', 'w')
    try:

        for num in range(0, 3, 1):
            print 'num = ', num
            if num == 0:
                plank_order_info_rows = db.order_info_xiaozhang1.find({'tvmid': {'$ne': ''}, 'openid': ''}).skip(
                    skip_number)
            elif num == 1:
                plank_order_info_rows = db.order_info_xiaozhang1.find({'tvmid': '', 'openid': {'$ne': ''}}).skip(
                    skip_number)
            elif num == 2:
                plank_order_info_rows = db.order_info_xiaozhang1.find(
                    {'tvmid': {'$ne': ''}, 'openid': {'$ne': ''}}).skip(skip_number)

            for order_row in plank_order_info_rows:
                # if loop_count % 100 == 0:
                # print loop_count, '\t', (datetime.datetime.now() - start_time), len(id_list)
                if loop_count % 10000 == 0:
                    # write to file
                    if ui_new_row != '':
                        output.write(str(ui_new_row))
                        output.flush()
                        ui_new_row = ''

                if num == 0:
                    each_id = order_row['tvmid']
                elif num == 1:
                    each_id = order_row['openid']
                elif num == 2:
                    each_id = order_row['tvmid']
                    open_id = order_row['openid']

                flg = id_list.has_key(each_id)
                loop_count += 1
                if flg == False:
                    # id_list[each_id] = (num + 1)

                    if num == 0:
                        user_order_rows = db.user_order_xiaozhang1.find({'tvmid': each_id})
                    elif num == 1:
                        user_order_rows = db.user_order_xiaozhang1.find({'openid': each_id})
                    elif num == 2:
                        user_order_rows = db.user_order_xiaozhang1.find({'tvmid': each_id, 'openid': open_id})

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

                        print 'Exception: '
                        print '\terror id: ', each_id, '; num: ', num
                        print '\tException Description:\t', str(Exception)
                        print '\tError message:\t\t', str(e)
                        print '\ttraceback.format_exc():\n%s' % traceback.format_exc()

                        f.write('Exception: \n')
                        f.write('error id: ' + str(each_id) + '; num: ' + str(num) + '\n')
                        f.write('\tException Description:\t' + str(Exception) + '\n')
                        f.write('\tError message:\t\t' + str(e) + '\n')
                        f.write('\ttraceback.format_exc():\n%s' % traceback.format_exc() + '\n')
                        f.flush()
                        skip_number += 1
                        continue

                    if num == 0:
                        order_count_per_user = db.order_info_xiaozhang1.find({'tvmid': each_id})
                    elif num == 1:
                        order_count_per_user = db.order_info_xiaozhang1.find({'openid': each_id})
                    elif num == 2:
                        order_count_per_user = db.order_info_xiaozhang1.find({'tvmid': each_id, 'openid': open_id})

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
                    # mall data is completed in this time

                    if num == 0:
                        user_info_rows = db.user_info_xiaozhang1.find({'tvm_id': each_id})
                    elif num == 1:
                        user_info_rows = db.user_info_xiaozhang1.find({'open_id': each_id})
                    elif num == 2:
                        user_info_rows = db.user_info_xiaozhang1.find({'tvm_id': each_id, 'open_id': open_id})

                    ok_flag = False
                    for ui in user_info_rows:
                        del ui['_id']
                        ui['Mall'] = mall_data
                        ui_new_row += str(ui) + '\n'
                        ok_flag = True

                    if ok_flag == False:
                        insert_json = {
                            'tvm_id': each_id,
                            'open_id': open_id,
                            'Mall': mall_data,
                        }
                        ui_new_row += str(insert_json) + '\n'

                    id_list[each_id] = (num + 1)
                    # file_write_list[each_id] = {'Mall': mall_data}

                else:
                    continue

        '''
        plank_order_info_rows = db.order_info_xiaozhang1.find({'tvmid': {'$ne': ''}, 'openid': ''})
        print plank_order_info_rows.count()

        for order_row in plank_order_info_rows:
            if loop_count % 10000 == 0:
                print loop_count, '\t', (datetime.datetime.now() - start_time), len(tvmid_list)
                # tvmid_list = []
            each_tvmid = order_row['tvmid']
            flg = tvmid_list.has_key(each_tvmid)
            loop_count += 1
            if flg == False:
                tvmid_list[each_tvmid] = 1
            else:
                continue

        print loop_count, '\t', (datetime.datetime.now() - start_time), 'len: ', len(tvmid_list)
        print 'STEP 1 time: ', (datetime.datetime.now() - start_time)
        step_1_loop_count = loop_count

        plank_order_info_rows = ''
        plank_order_info_rows = db.order_info_xiaozhang1.find({'tvmid': '', 'openid': {'$ne': ''}})
        print plank_order_info_rows.count()

        loop_count = 0
        step_2_start = datetime.datetime.now()
        for order_row in plank_order_info_rows:
            if loop_count % 10000 == 0:
                print loop_count, '\t', (datetime.datetime.now() - step_2_start), 'len: ', len(tvmid_list)
            each_tvmid = order_row['openid']
            flg = tvmid_list.has_key(each_tvmid)
            loop_count += 1
            if flg == False:
                tvmid_list[each_tvmid] = 2
            else:
                continue

        print loop_count, '\t', (datetime.datetime.now() - step_2_start), 'len: ', len(tvmid_list)
        print 'STEP 2 time: ', (datetime.datetime.now() - step_2_start)

        plank_order_info_rows = ''
        plank_order_info_rows = db.order_info_xiaozhang1.find({'tvmid': {'$ne': ''}, 'openid': {'$ne': ''}})
        print plank_order_info_rows.count()

        loop_count = 0
        step_3_start = datetime.datetime.now()
        for order_row in plank_order_info_rows:
            if loop_count % 10000 == 0:
                print loop_count, '\t', (datetime.datetime.now() - step_3_start), 'len: ', len(tvmid_list)
            each_tvmid = order_row['tvmid']
            flg = tvmid_list.has_key(each_tvmid)
            loop_count += 1
            if flg == False:
                tvmid_list[each_tvmid] = 2
            else:
                continue

        print loop_count, '\t', (datetime.datetime.now() - step_3_start), 'len: ', len(tvmid_list)
        print 'STEP 3 time: ', (datetime.datetime.now() - step_3_start)
        '''

        # user_info
        user_info_rows = db.user_info_xiaozhang1.find({})
        print 'user_info count: ', user_info_rows.count()

        # id_list['wxh58a55934090e62605842704b'] = 1
        # print id_list

        object_id = ''

        for ui in user_info_rows:
            try:
                # if total_count % 10000 == 0:
                #     print total_count
                if total_count % 5000 == 0:
                    if ui_new_row != '':
                        output.write(ui_new_row)
                        output.flush()
                        ui_new_row = ''

                object_id = ui['_id']
                del ui['_id']
                rows = ''
                if ui.has_key('tvm_id') == True:
                    tvmid_ui = str(ui['tvm_id'])
                else:
                    tvmid_ui = ''
                if ui.has_key('open_id') == True:
                    openid_ui = ui['open_id']
                else:
                    openid_ui = ''

                if id_list.has_key(tvmid_ui) == False and id_list.has_key(openid_ui) == False:
                    rows = str(ui)
                else:
                    total_count += 1
                    # print 'getted tvm_id: ', tvmid_ui, '; getted open_id: ', openid_ui
                    continue
            except BaseException, uie:
                print 'Exception: '
                print '\tobject_id: ', object_id, '; tvm_id: ', tvmid_ui, '; open_id: ', openid_ui
                print '\tException Description:\t', str(Exception)
                print '\tError message:\t\t', str(uie)
                print '\ttraceback.format_exc():\n%s' % traceback.format_exc()

                f.write('Exception: \n')
                f.write('\tobject_id: ' + str(object_id) + '; tvm_id: ' + str(tvmid_ui) + '; open_id: ' + str(
                    openid_ui) + '\n')
                f.write('\terror objectid: ' + str(object_id) + '\n')
                f.write('\tException Description:\t' + str(Exception) + '\n')
                f.write('\tError message:\t\t' + str(uie) + '\n')
                f.write('\ttraceback.format_exc():\n%s\t' % traceback.format_exc() + '\n')
                f.flush()
                continue

            # if id_list.has_key(tvmid_ui) == True:
            #     # add mall data to this row
            #     ui['Mall'] = mall_data
            #     rows = str(ui)
            # elif id_list.has_key(openid_ui) == True:
            #     # add mall data to this row
            #     ui['Mall'] = mall_data
            #     rows = str(ui)
            # else:
            #     # create a new data
            #     rows = str({
            #         'tvm_id': tvmid_ui,
            #         'open_id': openid_ui,
            #         'Mall': mall_data
            #     })

            ui_new_row += str(rows) + '\n'
            total_count += 1

    except BaseException, ee:
        print 'Exception: objectid: ', object_id, tvmid_ui, openid_ui
        print '\tException Description:\t', str(Exception)
        print '\tError message:\t\t', str(ee)
        print '\ttraceback.format_exc():\n%s' % traceback.format_exc()

        f.write('Exception: \n')
        f.write('\terror objectid: ' + str(object_id) + '\n')
        f.write('\tException Description:\t' + str(Exception) + '\n')
        f.write('\tError message:\t\t' + str(ee) + '\n')
        f.write('\ttraceback.format_exc():\n%s' % traceback.format_exc() + '\n')
        f.flush()

    finally:

        start_write_file = datetime.datetime.now()
        # output = open('data', 'w')
        output.write(str(ui_new_row))
        output.flush()
        output.close()
        # print 'write file use time: ', (datetime.datetime.now() - start_write_file)

        end_time = datetime.datetime.now()

        f = open('process_log.log', 'a')
        f.write('=======================================================\n')
        f.write('=======================================================\n\n')
        f.write(str('total time: ' + str(end_time - start_time) + '\n'))
        f.write(str('total loop count: ' + str(loop_count) + '\n'))
        f.write(str('total count user_info: ' + str(total_count) + '\n'))
        f.write(str('total list: ' + str(len(id_list)) + '\n'))
        f.flush()
        f.close()

        print '-------------------------------------------------------'
        print '-------------------------------------------------------'
        print str('total time: ' + str(end_time - start_time))
        print str('total loop count: ' + str(loop_count))
        print str('total count user_info: ' + str(total_count))
        print str('total list: ' + str(len(id_list)))
