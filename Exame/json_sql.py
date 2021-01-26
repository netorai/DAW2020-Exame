

cx_Oracle.init_oracle_client(lib_dir=r"C:\Oracle\instantclient_19_8")


api = 'http://nosql.hpeixoto.me/api/sensor/'

sensorid = 3001

while sensorid < 3006: 
    url = api + str(sensorid)
    #print(url)
    json_status = requests.get(url).status_code

    result = requests.get(url).json()
    #print(json.dumps(result, indent=2))
       
    if (json_status == 200 and result != None):
        print('API Status: Ok')

        try:
            conn = cx_Oracle.connect('sensors', 'sensors2020', 'localhost/orclpdb1.localdomain')
        except Exception as err:
            print('Erro ao criar a conexão ', err)
        else:
            print(conn.version)

        cur = conn.cursor()
        
        #CONFIGURANDO O DATE FORMAT
        cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'")

        care_list = result['careteam']
        #print(care_list)

        concatenar = ''
        for seq in range(len(care_list)):
        
            ID = str(care_list[seq]['id'])
            NOME = care_list[seq]['nome']
            concatenar += str(care_list[seq]['id'])+' / '

            care_insert = (ID, NOME)
            #print(care_insert)    
            cur.execute("INSERT INTO CARETEAM (ID, NOME) VALUES (:1,:2)", care_insert)
        #print(concatenar)

####### ARMAZENAR OS DADOS NA TABELA SENSOR ############
        NUMBER_OF_SENSORS = result["number_of_sensors"]
        SENSORID = result["sensorid"]
        SENSORNUM = result["sensornum"]
        TYPE_OF_SENSOR = result["type_of_sensor"]
        PATIENTID =result['patient']["patientid"]
        SERVICECOD = result["servicecod"]
        SERVICEDESC = result["servicedesc"]
        ADMDATE = result["admdate"]
        BED =  int(result["bed"]) 
        BODYTEMP = result["bodytemp"]
        BLOODPRESS = str(result['bloodpress']['systolic']) + '/' + str(result['bloodpress']['diastolic']) 
        BPM = result["bpm"]
        SATO2 = result["sato2"]
        CARETEAMID = concatenar
       
        sensor_insert = (NUMBER_OF_SENSORS, SENSORID, SENSORNUM, TYPE_OF_SENSOR, PATIENTID, SERVICECOD, SERVICEDESC, ADMDATE, BED, BODYTEMP, BLOODPRESS, BPM, SATO2, CARETEAMID)
        #print(sensor_insert)
        cur.execute("INSERT INTO SENSOR (NUMBER_OF_SENSORS, SENSORID, SENSORNUM, TYPE_OF_SENSOR, PATIENTID, SERVICECOD, SERVICEDESC, ADMDATE, BED, BODYTEMP, BLOODPRESS, BPM, SATO2, CARETEAMID, TIMESTAMP) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,CURRENT_TIMESTAMP)", sensor_insert)
        
 ####### ARMAZENAR OS DADOS NA TABELA PATIENT ############
        PATIENTID = result['patient']["patientid"]
        PATIENTNAME = result['patient']["patientname"]
        PATIENTBIRTHDATE = result['patient']["patientbirthdate"]
        PATIENTAGE = result['patient']["patientage"]

        patient_insert = (PATIENTID, PATIENTNAME, PATIENTBIRTHDATE, PATIENTAGE)
        #print(patient_insert)
        cur.execute("INSERT INTO PATIENT (PATIENTID, PATIENTNAME, PATIENTBIRTHDATE, PATIENTAGE) VALUES (:1,:2,:3,:4)", patient_insert)
       
        conn.commit() 

    else:
        print('API Status: Not Found')
    
    print(sensorid)
    if sensorid == 3005:
        cur.execute("DELETE FROM CARETEAM WHERE rowid NOT IN (SELECT MIN(rowid) FROM CARETEAM GROUP BY ID, NOME)")
        conn.commit() 
        cur.close()
        conn.close()
        break
    sensorid = sensorid + 1
   