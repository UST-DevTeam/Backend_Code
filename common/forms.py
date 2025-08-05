from base import *
def is_date(s):
    try:
        
        ctm.strtodate(s)
        # parser.parse(s)
        return True
    except Exception as e:
        return False
