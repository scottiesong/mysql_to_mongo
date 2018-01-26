#!/usr/bin/python
# -*- coding: UTF-8 -*-

import datetime
from pymongo import MongoClient

if __name__ == '__main__':
    client = MongoClient('mongodb://10.0.70.31', 27017)
    db = client['admin']
    db.authenticate('nosqluser', '654321', mechanism='SCRAM-SHA-1')
    db = client['devel2']

    plank_order_info_rows = 0

    id_list = {}  # []

    each_id = ''
    loop_count = 0
    # step_1_loop_count = 0

    start_time = datetime.datetime.now()

    try:

        for num in range(0, 3, 1):
            print 'num = ', num
            if num == 0:
                plank_order_info_rows = db.order_info_xiaozhang1.find({'tvmid': {'$ne': ''}, 'openid': ''})
            elif num == 1:
                plank_order_info_rows = db.order_info_xiaozhang1.find({'tvmid': '', 'openid': {'$ne': ''}})
            elif num == 2:
                plank_order_info_rows = db.order_info_xiaozhang1.find({'tvmid': {'$ne': ''}, 'openid': {'$ne': ''}})

            for order_row in plank_order_info_rows:
                if loop_count % 10000 == 0:
                    print loop_count, '\t', (datetime.datetime.now() - start_time), len(id_list)

                if num == 0:
                    each_id = order_row['tvmid']
                elif num == 1:
                    each_id = order_row['openid']
                elif num == 2:
                    each_id = order_row['tvmid']

                flg = id_list.has_key(each_id)
                loop_count += 1
                if flg == False:
                    id_list[each_id] = 1
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

    finally:

        start_write_file = datetime.datetime.now()
        output = open('data', 'w')
        output.write(str(id_list))
        output.flush()
        output.close()
        print 'write file use time: ', (datetime.datetime.now() - start_write_file)

        end_time = datetime.datetime.now()

        print 'total time: ', (end_time - start_time)
        print 'total loop count: ', loop_count
        print 'total list: ', len(id_list)
