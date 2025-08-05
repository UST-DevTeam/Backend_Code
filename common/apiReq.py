from base import *




def args_pagination(argu,tw=""):
  try:
    page = int(argu.get("page"))
    if not isinstance(page, int):
        page = 1
        
    limit = int(argu.get("limit"))
    if not isinstance(limit, int):
        limit = 50
    page = (page - 1) * limit
    

    return [
        {
            '$skip': page
        }, {
            '$limit': limit
        }
    ]
  except Exception as error:
      return [
        {
            '$skip': 0
        }, {
            '$limit': 50
        }
    ]

def argstostr(argu,tw=""):
    st=[]
    def_start=int(os.environ.get("DEFAULT_PAGINATION_START"))
    def_end=int(os.environ.get("DEFAULT_PAGINATION_END"))
    # print(argu,"arguargu")
    for i in argu:
        if(i=="start"):
            def_start=int(argu.get(i))
        elif(i=="end"):
            def_end=int(argu.get(i))
        else:
            st.append(f"{i}='{tw}.{argu.get(i)}'")

    return {
        "que":" AND ".join(st),
        "def_start":def_start,
        "def_end":def_end
    }
    
def countarra(collection,arra):
    agg = arra+[
        {
            '$match':{
                'deleteStatus':{'$ne':1}
            }
        }, {
            '$count': 'overall_table_count'
        }
        
        ]
    data = cmo.finding_aggregate(collection,agg)['data']
    if len(data):
        count = data[0]['overall_table_count']
    else:
        count = 0
    return [
        {
            '$addFields':{
                'overall_table_count':count
            }
        }
    ]


commonarra = [
            {
                '$match':{
                    'deleteStatus':{'$ne':1}
                }
            }, {
                '$facet': {
                    'outputFieldN': [
                        {
                            '$count': 'overall_table_count'
                        }
                    ], 
                    'outputFieldA': []
                }
            }, {
                '$unwind': {
                    'path': '$outputFieldA', 
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$match': {
                    'outputFieldA': {
                        '$ne': None
                    }
                }
            }, {
                '$addFields': {
                    'overall_table_count': {
                        '$arrayElemAt': [
                            '$outputFieldN', 0
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'outputFieldA.overall_table_count': '$overall_table_count.overall_table_count'
                }
            }, {
                '$replaceRoot': {
                    'newRoot': '$outputFieldA'
                }
            }
        ]

