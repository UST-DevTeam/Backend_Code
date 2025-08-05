def dateFilter(startDate, endDate,aggregate):
    return aggregate+[
        {
            '$addFields': {
                'createdTime': {
                    '$dateToString': {
                        'format': '%Y-%m-%d',
                        'date': { '$toDate': '$createdAt' }
                    }
                }
            }
        },
        {
            '$match': {
                '$expr': {
                    '$and': [
                        { '$gte': [ { '$toDate': '$createdTime' }, { '$toDate': startDate } ] },
                        { '$lte': [ { '$toDate': '$createdTime' }, { '$toDate': endDate } ] }
                    ]
                }
            }
        }
    ]
