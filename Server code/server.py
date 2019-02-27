import json
import ast
from flask import Flask
from flask import abort, redirect, url_for
import unicodedata
from runAuthentication import runAuthentication
import pywt
from DBHelper import DBHelper
from DBHelper import NumpyMySQLConverter

app = Flask(__name__)

@app.route('/')
def root():
    return redirect(url_for('home'))

@app.route('/home')
def home():
    return 'Welcome to home screen'

@app.route('/loginID/<string:jsonStr>')
def loginID(jsonStr):
    print 'incoming string-->', jsonStr
    json_obj = parse_data(jsonStr)
    intent = json_obj['INTENT']

    data = json_obj['DATA']
    arr = eval(data)
    id = json_obj['ID']
    sessionId = json_obj['SESSIONID']
    result = {}
    if intent == 'LOGIN':
        print 'login intent'
        is_authorized = authorize_brain_wave(arr, id)
        if is_authorized == 1:
            result['is_authorized'] = True
            userInfo = get_user_data(id)
            result['status'] = 'success'
            result['user_info'] = userInfo

        elif is_authorized == 0:
            result['status'] = 'success'
            result['is_authorized'] = False
        else:
            result['status'] = 'failure'
            result['message'] = 'Could not authenticate. Check database logs.'

    else:
        print 'unknown intent'
        result['status'] = 'failure'
    
    
    
    result_data = json.dumps(result)
    print result_data
    return result_data


@app.route('/registerID/<string:jsonStr>')
def registerID(jsonStr):

    result = {}
    print 'incoming string--> ', jsonStr
    json_obj = parse_data(jsonStr)
    intent = json_obj['INTENT']
    if intent == 'REGISTER':
        print 'register intent'
        print 'type of data-->', type(arr), type(arr[0])
        print 'size of list--->', len(arr)

        registerUserBrainwave(arr, id, sessionId)

        userInfo = get_user_data(id)
        result['user_info'] = userInfo
        result['status'] = 'success'

    else:
        print 'unknown intent'
        result['status'] = 'failure'

    
    result_data = json.dumps(result)
    print result_data
    return result_data    

def parse_data(jsonStr):
    new_working_str = jsonStr.encode('ascii', 'ignore')
    print 'working str', new_working_str
    json_obj = json.loads(new_working_str)
    print json_obj
    return json_obj

def process_data(jsonStr):
   
    new_working_str = jsonStr.encode('ascii', 'ignore')
    print 'working str', new_working_str
    json_obj = json.loads(new_working_str)
    print json_obj
    intent = json_obj['INTENT']
    print intent
    data = json_obj['DATA']
    arr = eval(data)
    print arr[0]
    return intent, arr

def authorize_user_id(id):

    db = DBHelper()
    cnx = db.getConn()
    if cnx is None:
        return None;
    else:
        cnx.set_converter_class(NumpyMySQLConverter)

        condExpr = 'UserID = ' + str(id)
        cursor = db.fetchFromWhere("UserInfo", condExpr, cnx)
        print 'row count->', cursor.rowcount
        if cursor.rowcount >0:
            return True
        else:
            return False;


def insert_data(data):
    name = data["NAME"]
    age = data["AGE"]
    gender = data["GENDER"]
    userId = registerUSerInfo(name, age, gender)
    return userId

def authorize_brain_wave(data, id):

    print 'type of data-->', type(data), type(data[0])
    result = runAuthentication(data,id)
    print 'result from process_DTW->', result
    return result

def get_brain_data(id, sessionid):

    db = DBHelper()
    cnx = db.getConn()
    cnx.set_converter_class(NumpyMySQLConverter)

    condExpr = 'ID = ' + str(id) + ' AND SESSIONID = \"' + str(sessionid) + '\"'
    cursor = db.fetchFromWhere("UBrainData", condExpr, cnx)
    series_list = cursor.fetchall()

    if len(series_list) >=1:
        data = []
        for row in series_list:
            data.append(float(row[3]))
        return data
    else:
        return None

def get_user_data(id):

    db = DBHelper()
    cnx = db.getConn()
    condExpr = ' UserID = ' + str(id)
    cursor = db.fetchFromWhere('UserInfo', condExpr, cnx) 
    if cursor.rowcount>0:
        userInfo = cursor.fetchone()
        print 'userInfo-->', userInfo
        return userInfo
    else:
        return None