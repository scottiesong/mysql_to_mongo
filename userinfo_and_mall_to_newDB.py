#!/usr/bin/python
# -*- coding: UTF-8 -*-

from bson.objectid import ObjectId
from pymongo import MongoClient

if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db = client['admin']
    db.authenticate('nosqluser', '654321', mechanism='SCRAM-SHA-1')
    db = client['devel']

    step_length = 1000

    while (1):
        order_info_rows = db.order_info_final_xiangzhang1.find({}).limit(step_length)

        row_count = order_info_rows.count()

        if row_count <= 0:
            exit(1)

        for oi_row in order_info_rows:
            ObjectId_oi = oi_row['_id']
            tvmid_oi = oi_row['tvm_id']
            openid_oi = oi_row['open_id']
            mall_data = oi_row['Mall']
            # print 'ObjectId_oi:', ObjectId_oi, 'tvm_id(oi): ', tvmid_oi, 'open_id(oi): ', openid_oi
            # print 'mall: '
            # print '\t', mall_data

            if tvmid_oi != '':
                # search by tvmid
                # print 'search by tvmid'
                user_info_rows = db.user_info_xiangzhang4.find({'tvm_id': tvmid_oi})
                ui_row_count = user_info_rows.count()

                if ui_row_count > 0:
                    for ui_row in user_info_rows:
                        # print ui_row
                        ObjectId_ui = ui_row['_id']
                        # print ObjectId_ui
                        db.user_info_xiangzhang4.update({'_id': ObjectId(ObjectId_ui)}, {'$set': {'Mall': mall_data}})
                    # print '---------------------------------------------------------------------------------------------'
                    db.order_info_final_xiangzhang1.remove({'_id': ObjectId(ObjectId_oi)})
                    continue

            if openid_oi != '':
                # search by openid
                # print 'search by openid'
                user_info_rows = db.user_info_xiangzhang4.find({'open_id': openid_oi})
                ui_row_count = user_info_rows.count()

                if ui_row_count > 0:
                    for ui_row in user_info_rows:
                        # print ui_row
                        ObjectId_ui = ui_row['_id']
                        db.user_info_xiangzhang4.update({'_id': ObjectId(ObjectId_ui)}, {'$set': {'Mall': mall_data}})
                    # print '---------------------------------------------------------------------------------------------'
                    db.order_info_final_xiangzhang1.remove({'_id': ObjectId(ObjectId_oi)})
                    continue

            # insert a new data
            insert_json = {
                'tvm_id': tvmid_oi,
                'open_id': openid_oi,
                'Mall': mall_data,
            }
            # print 'insert new data'
            # print insert_json
            db.user_info_xiangzhang4.insert(insert_json)

            # print ObjectId_oi
            db.order_info_final_xiangzhang1.remove({'_id': ObjectId(ObjectId_oi)})

            # print '---------------------------------------------------------------------------------------------'
