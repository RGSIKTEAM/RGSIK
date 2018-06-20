
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import http.client
import html #Unescape
import cgi
import base64 
import uuid
import json
import urllib #Кодирование ссылок в %
    
import importlib.util
spec = importlib.util.spec_from_file_location("module.name", "TestAPI.py")
MyAPI = importlib.util.module_from_spec(spec)
spec.loader.exec_module(MyAPI)

############################################################################
## Проверка отправленных СМС сообщений
############################################################################
def GetDataFromBaseForCheckSendSMS():
    SQLQuery="""Use Transport SELECT SMS.SMSID, SMS.SendSMSID,NeedDateSend,StatusName,ReasonText FROM SMS,Dictionary.dbo.ReasonSendSMS, 
    Dictionary.dbo.StatusSMS 
    WHERE SMS.Reason=Dictionary.dbo.ReasonSendSMS.ReasonID and SMS.StatusID=Dictionary.dbo.StatusSMS.StatusID and (DateExpiredRelevance>getdate() or DateExpiredRelevance is null)
            and NeedDateSend <getdate()
            and SMS.Statusid = 3
             /*and SMS.Statusid not in (0,2,4) and Attempt <= 2 */
             and Gate = 7
             and phone='89048307786'
             order by SMS.SMSID"""
             
    
    M = MyAPI.SELECTSQL(SQLQuery)
    print (M)
    M_SMS = M[1]
    
    for I in range(0,len(M_SMS)):
        CurRow = M_SMS[I]
        #print (CurRow)
        print ("SMSID="+CurRow[0])
        print ("SMSSendID="+CurRow[1])
        #print ("TextSMS="+CurRow[2])
        #print ("TextViber="+CurRow[3])
        
        #print  ("NewPhone=" + "7"+CurRow[1][1:11]) 
        #return ""
        print ("Проверка статуса СМС...")
        M=[]
        M=DecodeJSON(CheckStatus(CurRow[1]))
        print (str(M))
     
        
############################################################################
## Обновление статуса СМС в базе.
############################################################################
def UpdateStatus(SMSID,GroupID,Ans1,Ans2):
    M=[]
    M = SMSID,GroupID,Ans1,Ans2
    if (M[1]=="3"): #Удачная доставка
        MyAPI.ExecSQL("USE Transport UPDATE SMS SET StatusID=5, Answer='"+M[2]+": "+M[3]+"', datelastupdate=getdate() WHERE StatusID=3 and SendSMSID='"+SMSID+"'")
    if (M[1]!="3" and M[1]!="1" ): #Неудачная
        MyAPI.ExecSQL("USE Transport UPDATE SMS SET StatusID=4, Answer='"+M[2]+": "+M[3]+"', datelastupdate=getdate() WHERE StatusID=3 and SendSMSID='"+SMSID+"'")

############################################################################
## Получение не отправленных СМС сообщений
############################################################################
def GetDataFromBaseForSendSMS():
    SQLQuery="""Use Transport SELECT SMS.SMSID, SMS.Phone,SMS.TextSMS,SMS.TextMessenger,StatusName,ReasonText FROM SMS,Dictionary.dbo.ReasonSendSMS, 
    Dictionary.dbo.StatusSMS 
    WHERE SMS.Reason=Dictionary.dbo.ReasonSendSMS.ReasonID and SMS.StatusID=Dictionary.dbo.StatusSMS.StatusID and (DateExpiredRelevance>getdate() or DateExpiredRelevance is null)
            and NeedDateSend <getdate()
            and SMS.Statusid = 1
             and SMS.Statusid not in (0,2,4) and Attempt <= 0
             and Gate = 7
             order by NeedDateSend,SMS.DateCreateRecord"""
    
    M = MyAPI.SELECTSQL(SQLQuery)
    #print (M)
    M_SMS = M[1]
    
    
    for I in range(0,len(M_SMS)):
        CurRow = M_SMS[I]
        #print (CurRow)
        print ("SMSID="+CurRow[0])
        print ("Phone="+CurRow[1])
        print ("TextSMS="+CurRow[2])
        print ("TextViber="+CurRow[3])
        
        print  ("NewPhone=" + "7"+CurRow[1][1:11]) 
        #return ""
        print ("Отправляем СМС...")
        M=[]
        M=DecodeJSON(SendByScenario("7"+CurRow[1][1:11],CurRow[2],CurRow[3]))
        if (M[1]=="1"): #Удачная отправка
            MyAPI.ExecSQL("USE Transport UPDATE SMS SET StatusID=3,SendSMSID='"+M[0]+"', Answer='"+M[2]+": "+M[3]+"',Attempt=Attempt+1,date_send=getdate(), datelastupdate=getdate() WHERE SMSID="+CurRow[0])
        else: #Неудачная
            MyAPI.ExecSQL("USE Transport UPDATE SMS SET StatusID=2,SendSMSID='"+M[0]+"', Answer='"+M[2]+": "+M[3]+"',Attempt=Attempt+1,date_send=getdate(), datelastupdate=getdate() WHERE SMSID="+CurRow[0])
        
    
    
############################################################################
## Создание сценария отправки . Требуется только один раз. 
## На выходе будет ключ сценария, который надо использовать для рассылки
############################################################################
def CreateScenario():
    #Строка авторизации в формате b'ЛОГИН:ПАРОЛЬ'
    BHash=str(base64.standard_b64encode(b'RU-UD:070618').decode('UTF-8'))
    conn = http.client.HTTPSConnection("api.infobip.com")

    payload = """
    {
  "name":"Test Viber or SMS",
  "flow": [
    {
      "from": "RGSMedicine",
      "channel": "VIBER"
    },
    {
      "from": "RGSMedicine",
      "channel": "SMS"
    }
    
  ],
  "default": false
}
    """

    headers = {
        'content-type': "application/json",
        'authorization': "Basic " + BHash,
        'accept': "application/json"
        }

    conn.request("POST", "/omni/1/scenarios", payload, headers)

    res = conn.getresponse()
    data = res.read()
 
    print(data.decode("utf-8"))
                    

############################################################################
## Отправка сообщения согласно сценария на указанный номер указанные тексты.
############################################################################
def SendByScenario(Phone,TextSMS,TextViber):
    BulkGUID=str(uuid.uuid4()) 
    SMSGGUID=str(uuid.uuid4()) 
    ViberGUID=str(uuid.uuid4()) 
    
    #Строка авторизации в формате b'ЛОГИН:ПАРОЛЬ'
    BHash=str(base64.standard_b64encode(b'RU-UD:070618').decode('UTF-8'))
    
    #Сервер не любит спецсимволы (кавычки), не отправляет. Режем их.
    #Сценарий - сначала отправляем по Вайберу, затем по СМС
    payload = """
    { 
  "bulkId":"BULK-ID-"""+BulkGUID+"""",
  "scenarioKey":"F55B1BDF8B8187E0F92DC7309A0D0BF5",  
  "destinations":[ 
    
    { 
        "to":{
        "phoneNumber": """+'"'+Phone.replace('“','').replace('”','').replace('"','').replace('«','').replace('»','').strip()+'"'+"""
      }
    }
  ],
  "sms": {
    "text": """+'"'+TextSMS.replace('“','').replace('”','').replace('"','').replace('«','').replace('»','').replace('\r\n',' ').strip()+'"'+""",
     "language":{
       "languageCode":"RU"
     }
  },
  "viber": {
    "isPromotional": false,
    "text": """+'"'+TextViber.replace('“','').replace('”','').replace('"','').replace('«','').replace('»','').replace('\r\n',' ').strip()+'"'+"""
    
  }
}
    """
    
    headers = {
        'content-type': "application/json",
        'authorization': "Basic " + BHash,
        }
    conn = http.client.HTTPSConnection("api.infobip.com")
    conn.request("POST", "/omni/1/advanced", payload.encode("utf-8"), headers)

    res = conn.getresponse()
    data = res.read()

    Query="""
    USE [Logs]
INSERT INTO [dbo].[InfoBIP]
           (
           URL
           ,Headers
           ,[Request]
           ,[Answer]
           ,[DateCreateRecord])
     VALUES
           (
           '/omni/1/advanced'
           ,'"""+str(headers).replace("'",'"')+"""'
           ,'"""+payload+"""'
           ,'"""+data.decode("utf-8")+"""'
           ,GetDate())
    """
    MyAPI.ExecSQL(Query)
    return(data.decode("utf-8"))
      

############################################################################
## Отправка сообщения согласно сценария на указанный номер указанные тексты.
############################################################################
def CheckStatus(SendSMSID):
    BulkGUID=str(uuid.uuid4()) 
    SMSGGUID=str(uuid.uuid4()) 
    ViberGUID=str(uuid.uuid4()) 
    #Строка авторизации в формате b'ЛОГИН:ПАРОЛЬ'
    BHash=str(base64.standard_b64encode(b'RU-UD:070618').decode('UTF-8'))
   
    payload = """
    { 
  "messageId":"""+'"'+SendSMSID+'"'+""",
  "limit":"""+'"'+str(1)+'"'+"""
  
}
    """

    headers = {
        'content-type': "application/json",
        'authorization': "Basic " + BHash,
        }
    conn = http.client.HTTPSConnection("api.infobip.com")
    conn.request("GET", "/omni/1/reports", payload.encode("utf-8"), headers)

    res = conn.getresponse()
    data = res.read()

    Query="""
    USE [Logs]
INSERT INTO [dbo].[InfoBIP]
           (
           URL
           ,Headers
           ,[Request]
           ,[Answer]
           ,[DateCreateRecord])
     VALUES
           (
           '/omni/1/reports'
           ,'"""+str(headers).replace("'",'"')+"""'
           ,'"""+payload+"""'
           ,'"""+data.decode("utf-8")+"""'
           ,GetDate())
    """
    MyAPI.ExecSQL(Query)
    return(data.decode("utf-8"))
            
############################################################################
## Простая отправка СМС на номер
############################################################################
def SendSMS(Phone,Text):
    #Строка авторизации в формате b'ЛОГИН:ПАРОЛЬ'
    BHash=str(base64.standard_b64encode(b'RU-UD:070618').decode('UTF-8'))
    print (BHash)
    #return 
    conn = http.client.HTTPSConnection("api.infobip.com")

    payload = "{\"from\":\"RGSMedicine\",\"to\":\""+html.escape(Phone)+"\",\"text\":\""+html.escape(Text)+"\"}"

    headers = {
        'authorization': "Basic " + BHash,
        'content-type': "application/json",
        'accept': "application/json"
        }
    print (payload.encode("utf-8").decode('UTF-8')  )
    return 
    conn.request("POST", "/sms/1/text/single", payload.encode("utf-8"), headers)

    res = conn.getresponse()
    data = res.read()
    
    M=[]
    M=DecodeJSONCheck(data.decode("utf-8"))
    
    
############################################################################
## Декодировка JSON
############################################################################    
def DecodeJSON(CurJSON):        
    print ('JSON='+CurJSON)
    f = open('/tmp/jsonInfoBIP.tmp', 'w')
    f.write(CurJSON)
    f.close()

    data=""
    Napr=""
    Who=""
    Where=""
    A=0
    with open('/tmp/jsonInfoBIP.tmp') as fp:
        data = json.load(fp)
        fp.close()
        
    for I in data['messages']:
        #A=A+1
        #print ("Обрабатываем номер: " + str(A))
        print ("messageId = "+ str (I['messageId']))
        print ("groupId = "+ str (I['status']['groupId']))
        print ("name = "+ str (I['status']['name']))
        print ("description = "+ str (I['status']['description']))
        return str (I['messageId']),str (I['status']['groupId']),str (I['status']['name']),str (I['status']['description'])


############################################################################
## Декодировка JSON
############################################################################    
def DecodeJSONCheck(CurJSON):        
    print ('JSON='+CurJSON)
    f = open('/tmp/jsonInfoBIP.tmp', 'w')
    f.write(CurJSON)
    f.close()
    #data = json.load(f)
    data=""
    Napr=""
    Who=""
    Where=""
    A=0
    with open('/tmp/jsonInfoBIP.tmp') as fp:
        data = json.load(fp)
        fp.close()
        
    #print('Найдено записей:' + data['iTotalDisplayRecords'])
    for I in data['results']:
        #A=A+1
        #print ("Обрабатываем номер: " + str(A))
        print ("messageId = "+ str (I['messageId']))
        print ("groupId = "+ str (I['status']['groupId']))
        print ("name = "+ str (I['status']['name']))
        print ("description = "+ str (I['status']['description']))
        UpdateStatus( str (I['messageId']),str (I['status']['groupId']),str (I['status']['name']),str (I['status']['description']))
        
############################################################################    
## ТОЧКА ВХОДА В СКРИПТ
############################################################################    

#Отправка простого СМС         
#SendSMS("79048307786","Тествое СМС")
#Пример ответа
#{"messages":[{"to":"79048307786","status":{"groupId":5,"groupName":"REJECTED","id":12,"name":"REJECTED_NOT_ENOUGH_CREDITS","description":"Not enough credits"},"smsCount":1,"messageId":"2285429234203536773"}]}

#Создание сценария. Требуется один раз для получения ID сценария для отправки сначала на Вайбер, потом на СМС
#CreateScenario()
#Пример ответа
#{"key":"1FC1CFEC20987CA2927B282050C8E5DB","name":"Send SMS or Viber","flow":[{"from":"RGSMedicine","channel":"SMS"},{"from":"RGSMedicine","channel":"VIBER"}],"default":true}
#{"key":"F55B1BDF8B8187E0F92DC7309A0D0BF5","name":"Test Viber or SMS","flow":[{"from":"RGSMedicine","channel":"VIBER"},{"from":"RGSMedicine","channel":"SMS"}],"default":false}



#Запуск рассылки  
GetDataFromBaseForSendSMS()
#Пример ответа
#{"bulkId":"BULK-ID-b1cdaf41-637d-490f-a3c4-3f79040c3825","messages":[{"to":{"phoneNumber":"79048307786"},"status":{"groupId":1,"groupName":"PENDING","id":7,"name":"PENDING_ENROUTE","description":"Message sent to next instance"},"messageId":"40b033bf-fd75-4d72-ab12-a72891d9034a"}]}
#{"bulkId":"BULK-ID-d5efe1a1-2531-4e26-ad25-5b598119d6a0","messages":[{"to":{"phoneNumber":"79048307786"},"status":{"groupId":1,"groupName":"PENDING","id":7,"name":"PENDING_ENROUTE","description":"Message sent to next instance"},"messageId":"34e99298-881c-4dcc-99d7-a2f488ce920f"}]}

#Получение отчетов до первого пустого результа. Внимание! Отчеты о доставке сообщений получаются только один раз! 
#При следующем запросе будут уже новые результаты. 
for I in range(0,10000):
    R=CheckStatus("")
    if (R=="""{"results":[]}"""):
        break
    else:
        DecodeJSONCheck(R)

#Перепроверка логов по сохраненным результатам. Если вдруг при сохранение в реальном времени произошла ошибка
SQLQuery="""USE LOGS SELECT Answer,LogID
FROM Logs.dbo.InfoBIP
WHERE Answer LIKE '%result%'
"""        
M = MyAPI.SELECTSQL(SQLQuery)
M_SMS = M[1]

for I in range(0,len(M_SMS)):
    CurRow = M_SMS[I]
    DecodeJSONCheck(CurRow[0])
