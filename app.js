const axios = require('axios');

const express = require('express')
const app = express()

const kFREQUENCY_CHECK_DURATION = 10
const kREQUEST_FREQUENCY_THRESHOLD = 10
const kDB_QUERY_DURATION_LIMIT = 2678400
const kINFLUXDB_QUERY_LIMIT = 2500

let MongoClient = require('mongodb').MongoClient;

const kMONGODB_URL = 'mongodb://localhost:27017'
const kMONGODB_DB = 'your_db'
const kMONGODB_TOKEN_COLLECTION = 'your_collection'
const kMONGODB_SENSOR_COLLECTION = 'your_collection'

var tokenHub = new Map()

//list去除重複
function remove_duplicates(arr) {
    var obj = {};
    var ret_arr = [];
    for (var i = 0; i < arr.length; i++) {
        obj[arr[i]] = true;
    }
    for (var key in obj) {
        ret_arr.push(key);
    }
    return ret_arr;
}

// 檢查api傳入之參數
function checkParameters(req, res, next) {
    let start_time, end_time, token, offset
    let appids = []

    //檢查必要參數start_time
    if (req.query.hasOwnProperty('start_time')) {
        start_time = req.query['start_time']
        console.log('start_time:' + start_time)
        req.start_time = start_time
    } else {
        return res.status(400).json({'error': 'can\'t find start_time'})
    }
    // 檢查必要參數end_time
    if (req.query.hasOwnProperty('end_time')) {
        end_time = req.query['end_time']
        console.log('end_time:' + end_time)
        req.end_time = end_time
    } else {
        return res.status(400).json({'error': 'can\'t find end_time'})
    }

    // 查詢duration(最長期間),超過返回錯誤
    if (end_time - start_time > kDB_QUERY_DURATION_LIMIT) {
        return res.status(400).json({'error': 'query duration too long'})
    }

    // 檢查並取得token
    let rawToken;
    if (req.headers.hasOwnProperty('authorization')) {
        // console.log('authorization:' + req.headers['authorization'])
        rawToken = req.headers['authorization']
        token = rawToken.split(' ')[1];
        console.log('token:' + token)
        req.token = token
    } else {
        return res.status(400).json({'error': 'can\'t find token'})
    }

    //判斷是否有offset及offset必需>=0,將offset當參數傳入
    if (req.query.hasOwnProperty('offset')) {
        offset = req.query['offset']
        // console.log('offset:' + offset)
        offset = parseInt(offset, 10)
        console.log('offset:' + offset)
        if (isNaN(offset)) {
            res.status(404).json({'error': 'offset format error'})
            return
        }
        if (offset < 0) {
            res.status(404).json({'error': 'offset can\'t be negative'})
            return
        }
    } else {
        offset = 0
        console.log('offset:' + offset)
    }
    req.offset = offset

    //判斷是否有appid, 不帶appid代表查詢全部appid,將appid組成list傳入, 不帶appid傳入空陣列
    if (req.query.hasOwnProperty('appid')) {
        if (Array.isArray(req.query['appid'])) {
            appids = req.query['appid']
            console.log('multiple appid')
        } else {
            appids.push(req.query['appid'])
            console.log('single appid')
        }
    }
    console.log("appids:", appids)
    req.appids = appids

    next()
}

// 連mongodb用token取user,以token從mongodb反查id
function getUserFromMongoDB(req, res, next) {
    MongoClient.connect(kMONGODB_URL, function (err, db) {
        if (err) {
            return res.status(400).json({'error': err.toString()})
        }
        const dbo = db.db(kMONGODB_DB)
        dbo.collection(kMONGODB_TOKEN_COLLECTION).find({'token': req.token}, {}).toArray(function (err, result) {
            if (err) {
                return res.status(400).json({'error': err.toString()})
            }

            if (result.length > 1) {
                return res.status(400).json({'error': 'find multiple user according to this token'})
            }
            if (result.length === 0) {
                return res.status(400).json({'error': 'token error'})
            }

            let user = result[0]['id']
            console.log('user:' + user)
            req.user = user
            db.close()
            next()
        })
    })
}

// 檢查同一個token是否密集查詢
function requestFreqCheck(req, res, next) {
    // #判斷token是否過於密集呼叫
    // 取得當下timestamp
    let now = Math.floor(Date.now() / 1000)
    console.log('now:' + now)
    // 判斷token是否已被紀錄
    if (tokenHub[req.token]) {
        console.log("old token")
        // 清除過期紀錄
        for (let i = tokenHub[req.token].length - 1; i >= 0; i--) {
            if (tokenHub[req.token][i] < now - kREQUEST_FREQUENCY_THRESHOLD) {
                tokenHub[req.token].splice(i, 1)
            }
        }
        // 檢查當前長度, 超過顯示Error
        if (tokenHub[req.token].length >= kFREQUENCY_CHECK_DURATION) {
            return res.status(400).json({'error': 'request too frequently'})
        } else {
            tokenHub[req.token].push(now)
        }
    } else {// 不存在的token新增
        console.log("new token")
        tokenHub[req.token] = [now]
    }
    next()
}

// GetUserAllAppidFromInflux 只有user時從influxdb取得appid list
function getUserAllAppidFromInflux(req, res, next) {
    let addParams = function (data) {
        let appidArray = []
        for (let i = 0; i < data.length; i++) {
            if (data[i][0].startsWith(req.user)) {
                appidArray.push(data[i][0])
            }
        }
        appidArray.sort()
        req.appids = appidArray
        next()
    }

    // request沒帶appid
    if (req.appids.length === 0) {
        axios
            .get('http://localhost:8086/query', {
                params: {
                    'db': 'your_db',
                    'q': 'show measurements'
                }
            })
            .then(response => {
                addParams(response.data.results[0].series[0].values)
            })
            .catch(err => {
                return res.status(400).json({'error': err.toString()})
            })
    } else {// request有帶appid加上user
        console.log('with appids: ' + req.appids)
        let userAppid = []
        for (let i = 0; i < req.appids.length; i++) {
            userAppid.push(req.user + '_' + req.appids[i])
        }
        userAppid.sort()
        req.appids = userAppid
        next()
    }
}

// GetInfluxDataCounts 查詢符合條件每張表的筆數
function getInfluxDataCounts(req, res, next) {
    let addParams = function (data) {
        let tableCount = {}
        for (let i = 0; i < data.length; i++) {
            tableCount[data[i].name] = data[i].values[0][1]
        }
        req.tableCounts = tableCount
        next()
    }
    let measurements = req.appids.join(`","`)
    let query = `SELECT Count(time_value)
                 FROM "${measurements}"
                 where time >=${req.start_time * 1000000000}
                   and time
                     <${req.end_time * 1000000000}`
    axios
        .get('http://localhost:8086/query', {
            params: {
                'db': 'loradb',
                'q': query
            }
        }).then(response => {
        addParams(response.data.results[0].series)
    })
        .catch(err => {
            console.log(err)
            return res.status(400).json({'error': err.toString()})
        })
}

// createTableQueryCondition 返回多表查詢條件整合及符合條件總筆數
// 單次查詢最多跨三張表
function createTableQueryCondition(req, res, next) {
    let total = 0
    let insufficient = 0
    let offset = req.offset
    let limit = kINFLUXDB_QUERY_LIMIT
    for (let key in req.tableCounts) {
        total += req.tableCounts[key]
    }
    console.log('total', total)
    console.log('offset', offset)
    console.log('limit', limit)
    req.total = total

    // 依input_offset及limit建map蒐集此次request會用到的查詢條件
    let queryConditionMap = {}
    if (total <= offset) {
        return res.status(400).json({'error': 'offset greater than total'})
    }

    let thisQueryReturnAmount = 0
    // 最多跨三張表
    for (let i = 0; i < req.appids.length; i++) {
        if (!req.tableCounts.hasOwnProperty(req.appids[i])) {
            continue
        }
        let thisAppid = req.appids[i]
        let thisCount = req.tableCounts[thisAppid]
        thisQueryReturnAmount += thisCount - offset

        if (offset > 0) {
            if (offset >= thisCount) { // 此表已讀過
                //	offset 自減此表數量跳到下一張表計算
                offset -= thisCount
                continue
            } else { // Offset<此表數量,代表要讀此表
                if (offset + limit <= thisCount) { // 代表此表數量足夠,是這次request最後一張要讀的表
                    // 把表名,offset,limit加入influx查詢條件
                    queryConditionMap[thisAppid] = {
                        'offset': offset,
                        'limit': limit,
                    }
                    break
                } else { // offset+limit>此表數量,代表此表數量不足
                    // 把表名,offset,limit加入influx查詢條件
                    queryConditionMap[thisAppid] = {
                        'offset': offset,
                        'limit': thisCount - offset,
                    }
                    insufficient = limit - (thisCount - offset)
                    offset = 0
                    // console.log(292, "insufficient", insufficient)
                    continue
                }
            }
        }

        if (offset === 0) {
            if (insufficient === 0) {
                if (limit <= thisCount) {
                    queryConditionMap[thisAppid] = {
                        'offset': offset,
                        'limit': limit,
                    }
                    break
                } else { // limit>thisCount
                    queryConditionMap[thisAppid] = {
                        'offset': offset,
                        'limit': thisCount,
                    }
                    insufficient = limit - thisCount
                    // continue
                }
            } else { //insufficient>0
                if (insufficient <= thisCount) {
                    queryConditionMap[thisAppid] = {
                        'offset': offset,
                        'limit': insufficient,
                    }
                    break
                } else { // insufficient>此表數量
                    queryConditionMap[thisAppid] = {
                        'offset': offset,
                        'limit': thisCount,
                    }
                    insufficient -= thisCount
                    // continue
                }
            }
        }
        // 最多查詢三張表
        if (Object.keys(queryConditionMap).length === 3) {
            break
        }
    }
    console.log('queryConditionMap:',queryConditionMap)
    req.queryConditionMap = queryConditionMap
    if (req.offset + thisQueryReturnAmount >= total) {
        req.offset = total
    } else {
        req.offset = req.offset + thisQueryReturnAmount
    }
    next()
}


function getInfluxData(req, res, next) {
    let numberOfTotalQuery = Object.keys(req.queryConditionMap).length
    console.log('number of total query', numberOfTotalQuery)
    let queryCount = 0
    let result = []
    let deveuiList = []
    let appendResult = function (data) {
        queryCount++
        for (let i = 0; i < data.length; i++) {
            let singleRow = {}
            singleRow['app_id'] = data[i].name
            let values = data[i].values
            let columns = data[i].columns
            for (let valueIndex = 0; valueIndex < values.length; valueIndex++) {
                let singleValue = values[valueIndex]
                for (let columnIndex = 1; columnIndex < columns.length; columnIndex++) {
                    if (columns[columnIndex] === 'deveui') {
                        deveuiList.push(singleValue[columnIndex])
                    }
                    singleRow[columns[columnIndex]] = singleValue[columnIndex]
                }
                const clone = Object.assign({}, singleRow)
                result.push(clone)
            }
        }
        if (queryCount === numberOfTotalQuery) {
            req.result = result
            req.deveuiList = remove_duplicates(deveuiList)
            next()
        }
    }
    for (let appid in req.queryConditionMap) {
        let query = `SELECT time_value as timestamp, battery_percentage, battery_voltage, deveui, freq, gateway_eui, rssi, snr, temperature, uplink_count as upctr
                     FROM "${appid}"
                     where time >=${req.start_time * 1000000000}
                       and time
                         <${req.end_time * 1000000000}
                         limit ${req.queryConditionMap[appid]['limit']}
                     offset ${req.queryConditionMap[appid]['offset']}
        `
        axios
            .get('http://localhost:8086/query', {
                params: {
                    'db': 'loradb',
                    'q': query
                }
            }).then(response => {
            appendResult(response.data.results[0].series)
        })
            .catch(err => {
                console.log(err)
                return res.status(400).json({'error': err.toString()})
            })
    }
}

// 查詢mongodb取得溫度上下門檻
function getAlarmMaxMinTemperature(req, res, next) {
    MongoClient.connect(kMONGODB_URL, function (err, db) {
        if (err) {
            return res.status(400).json({'error': err.toString()})
        }
        const dbo = db.db(kMONGODB_DB)
        dbo.collection(kMONGODB_SENSOR_COLLECTION).find({'sensor_id': {$in: req.deveuiList}}, {
            sensor_id: 1,
            sensor_max_temp: 1,
            sensor_min_temp: 1,
            _id: 0
        }).toArray(function (err, mongoTemperatureResult) {
            if (err) {
                return res.status(400).json({'error': err.toString()})
            }

            db.close()
            req.mongoTemperatureResult = mongoTemperatureResult
            next()
        })
    })
}

//把從mongo取得的溫度整合進原本的result
function finalReturn(req, res, next) {
    let finalResult = []
    let tempDict = {}
    for (let i = 0; i < req.mongoTemperatureResult.length; i++) {
        tempDict[req.mongoTemperatureResult[i]['sensor_id']] = {
            'max': req.mongoTemperatureResult[i]['sensor_max_temp'],
            'min': req.mongoTemperatureResult[i]['sensor_min_temp']
        }
    }
    for (let i = 0; i < req.result.length; i++) {
        let singleRow = req.result[i]
        let deveui = req.result[i].deveui
        if (tempDict.hasOwnProperty(deveui)) {
            let min = tempDict[deveui].min
            let max = tempDict[deveui].max
            singleRow['alarm_min_temp'] = min
            singleRow['alarm_max_temp'] = max
            if (singleRow['temperature'] >= max || singleRow['temperature'] <= min) {
                singleRow['alarm_flag'] = 1
            } else {
                singleRow['alarm_flag'] = 0
            }
        } else {
            singleRow['alarm_min_temp'] = null
            singleRow['alarm_max_temp'] = null
            singleRow['alarm_flag'] = 0
        }
        finalResult.push(singleRow)
    }
    res.json({
        'data': finalResult,
        'offset': req.offset,
        'total': req.total,
        // 'tempDict': tempDict,
        // 'deveui': req.deveuiList
    })
}

app.get('/api/haccp/tcat/get_data',
    checkParameters,
    getUserFromMongoDB,
    requestFreqCheck,
    getUserAllAppidFromInflux,
    getInfluxDataCounts,
    createTableQueryCondition,
    getInfluxData,
    getAlarmMaxMinTemperature,
    finalReturn
)


const PORT = process.env.PORT || 3000
app.listen(PORT, () => console.log(`server listening on port ${PORT}`))



