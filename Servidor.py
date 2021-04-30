import pandas as pd
import datetime
import http.client, urllib.request, urllib.parse, urllib.error, base64
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
import requests
import json  
import codecs
import numpy as np
import wget
import http.client, urllib.request, urllib.parse, urllib.error, base64

#************************************Bucle general********************************************************
def UpdateDatabase():
    print("Comenzo...")
    '''try:
        datasetFinalTweet()
        print("Los tweet se cargaron correctamente...")
    except:
        print("Error al cargar los Tweet")'''

    datasetFinalTweet()

    try:
        bingNews()
        print("Las noticias se cargaron correctamente...")
    except:
        print("Error al cargar las noticias")
    try:
        guardarDataCovid()
        print("Los datos de datacovid se cargaron correctamente...")
    except:
        print("Los datos de datacovid no se cargaron correctamente...")

    return

#************************************Actualizar repositorio***********************************************
#************************************Analisis Twitter*******************************************
def getKeys():
    f = open('key.json','r')
    keys = f.read()
    jkeys = json.loads(keys)
    return jkeys
#************************************Analisis Twitter*******************************************
#************************************Actualizar Datos Twiter*******************************************
#Esta funcion devuelve la api necesaria para hacer todas las consultas requeridas
def APITWEET():
    # Declaramos nuestras Twitter API Keys:
    keys = getKeys()
    #ACCESS_TOKEN = '1230251564616515586-2KqPsCG2mIJp3irRjENgHpCfQUxTUg'
    #ACCESS_TOKEN_SECRET = '6PJfMtYGY7w6csiIX9m1S5jFEKNZ3hE9PVkHKeN1S14iM'
    #CONSUMER_KEY = 'koO4XqTuWFr5ADGcE8kjIkVoU'
    #CONSUMER_SECRET = '3F4sk9qU8zbKBROuLPUUj1uvE2YuhseXPe0ahMQoivg4icN5bL'  
    ACCESS_TOKEN = keys['twitter']['token_acceso']
    ACCESS_TOKEN_SECRET = keys['twitter']['secreto_token_acceso']
    CONSUMER_KEY = keys['twitter']['clave_api']
    CONSUMER_SECRET = keys['twitter']['clave_secreta_api']
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    return api
#Tomar la fecha que viene en los tweet en formato cadena de texto y transformarla en formato datetime
def FechaTweeter(palabra):
    anio = int(palabra[-4:])
    meses = {
        "Jan":1,
        "Feb":2,
        "Mar":3,
        "Apr":4,
        "May":5,
        "Jun":6,
        "Jul":7,
        "Aug":8,
        "Sep":9,
        "Oct":10,
        "Nov":11,
        "Dec":12
    }
    mes = meses[palabra[4:7]]
    dia = int(palabra[8:10])
    hora = int(palabra[11:13]) 
    minuto = int(palabra[14:16])
    segundo = int(palabra[17:19])
    return datetime.datetime(anio,mes,dia,hora,minuto,segundo) - datetime.timedelta(hours = 6)
#Limpiar el campo source
def depurarFuenteTweet(palabra):
    salida = palabra.replace('<a href="https://about.twitter.com/products/tweetdeck" rel="nofollow">','').replace("</a>","")
    salida = salida.replace('<a href="http://twitter.com/download/iphone" rel="nofollow">',"")
    salida = salida.replace('<a href="https://studio.twitter.com" rel="nofollow">',"")
    salida = salida.replace('<a href="https://mobile.twitter.com" rel="nofollow">',"")
    salida = salida.replace('<a href="http://twitter.com" rel="nofollow">',"")
    salida = salida.replace('<a href="http://twitter.com/download/android" rel="nofollow">',"")
    salida = salida.replace('<a href="https://www.hootsuite.com" rel="nofollow">',"")
    #salida = salida.replace('<a href=""http://twitter.com/download/android"" rel=""nofollow"">',"")
    return salida
#A partir del usuario user se devuelve en una lista los ultimos 10 tweet
def get_tweetConFecha(user, api = APITWEET()):
    return list(api.user_timeline(screen_name = user, count= 10))

#Crea un dataset con los ultimos 10 tweet
def definirDatasetPorCuenta(cuenta):
#lista = get_tweetConFecha("colmedchile")
    lista = get_tweetConFecha(cuenta)
    salida = []
    for i in lista:  
        jsonObject = i._json.copy()
        datos = {
                    "Contenido" : jsonObject["text"], 
                    "IR" : "https://twitter.com/i/web/status/" + jsonObject["id_str"], 
                    "Fecha" : FechaTweeter(jsonObject["created_at"]).strftime("%d/%m/%Y %H:%M:%S"),
                    "Dispositivo" : depurarFuenteTweet(jsonObject["source"]),
                    "Likes" : jsonObject["favorite_count"],
                    "Retweets" : jsonObject["retweet_count"],
                    "Entidad" : jsonObject["user"]["name"],
                    "Hora" : FechaTweeter(jsonObject["created_at"]).strftime("%H:%M:%S"),
                    "Foto": jsonObject["user"]["profile_image_url"].replace("_normal.","."),
                    "FechaAux": FechaTweeter(jsonObject["created_at"])
                }
        salida.append(datos.copy())
    data = pd.DataFrame(salida)
    return data
#Crea un dataset con todas las cuentas en la lista cuentas
def datasetFinalTweet():
    cuentas = [
                "GuatemalaGob",
                "mingobguate",
                "ejercito_gt",
                "GUATEMINECO", # Cuenta inexistente
                "MinFinGT",
                "MinSaludGuate",
                "MigracionGuate",
                "opsoms",
                "MinexGt"
                ]
    salida = []
    for i in cuentas:
        salida.append(definirDatasetPorCuenta(i))
    data = pd.concat(salida)
    data = data.sort_values(by=['FechaAux'])
    del data["FechaAux"]
    data.to_csv("Twitter/Tweet.csv", index=False)
    return data

#tweepy.Cursor(api.search, q='#मराठी OR #माझाक्लिक OR #म')
#tweepy.Cursor(api.friends)
#tweepy.Cursor(api.home_timeline)
#tweepy.Cursor(api.search, url)
#tweepy.Cursor(api.friends, user_id=user_id, count=200).items()
#tweepy.Cursor(api.mentions_timeline, user_id=user_id, count=200).items()
#######https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
#************************************Actualizar Datos Twiter*******************************************
#************************************Actualizar BING NEWS*******************************************
"""
Argentina	AR
Australia	AU
Austria	AT
Belgium	BE
Brazil	BR
Canada	CA
Chile	CL
Denmark	DK
Finland	FI
France	FR
Germany	DE
Hong Kong SAR	HK
India	IN
Indonesia	ID
Italy	IT
Japan	JP
Korea	KR
Malaysia	MY
Mexico	MX
Netherlands	NL
New Zealand	NZ
Norway	NO
China	CN
Poland	PL
Portugal	PT
Philippines	PH
Russia	RU
Saudi Arabia	SA
South Africa	ZA
Spain	ES
Sweden	SE
Switzerland	CH
Taiwan	TW
Turkey	TR
United Kingdom	GB
United States	US
"""
#Entrega la fecha en un formato dia-mes-año hora:minutos
def fechaCorrecta(i):
    año = i[0:4]
    mes = i[5:7]
    dia = i[8:10]
    hora = i[11:13]
    minuto = i[14:16]
    return dia + "-" + mes + "-" + año + " " + hora + ":" + minuto

#Reemplaza por nada
def reemplazarFinal(i):
    return i.replace("&pid=News","")

def bingNews(pais = "Guatemala"):
    #pais = "Chile"
    headers = {
        # Request headers
        #'Ocp-Apim-Subscription-Key': 'b091fbaeb9f94255b542befc3ecff8b8',
        #Poner la key necesaria para hacer las consultas
        'Ocp-Apim-Subscription-Key': 'a9b5b1527a7b43929d7e15a383b1583a',
    }

    params = urllib.parse.urlencode({
        # Request parameters
        'q':  'covid-19 coronavirus ' + pais + ' loc:gt FORM=HDRSC4',
        'count': '40',
        'offset': '0',
        'mkt': 'es-US',
        'safeSearch': 'Moderate',
        "sortBy": "Date"
    })

    #conn = http.client.HTTPSConnection('api.cognitive.microsoft.com')
    conn = http.client.HTTPSConnection('dataintelligence.cognitiveservices.azure.com')
    conn.request("GET", "/bing/v7.0/news/search?%s" % params, "{body}", headers)
    response = conn.getresponse()
    #data = response.read()

    decoded_data=codecs.decode(response.read(), 'utf-8-sig')
    d = json.loads(decoded_data)
    conn.close()
    aux =  d['value']
    salida = []
    for i in aux:
        try:
            i["imagen"] = i["image"]["thumbnail"]["contentUrl"]
            i["pais"] = "Guatemala"
            try:
                i["Fuente"] = i['provider'][0]["name"]
            except:
                pass
            salida.append(i.copy())
        except:
            pass
    data = pd.DataFrame(salida)[["name","url","description","datePublished","imagen","pais","Fuente"]]
    data["datePublished"] = data["datePublished"].apply(fechaCorrecta)
    data["imagen"] = data["imagen"].apply(reemplazarFinal)
    data[::-1].to_csv("bing/news/Guatemala.csv",index=False)
    return
#************************************Actualizar BING NEWS*******************************************
#************************************Actualizar Datos de la organizacion*******************************************
def guardarDataCovid():
    #Casos GT v1.xlsx
    print("Función guardarDataCovid")
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!62456&parId=9F999E057AD8C646!62453&authkey=!AkePW7UW1KXQkMM&app=Excel"
    
    #urllib.request.urlretrieve(url, "../Datos_Guatemala/datacovidgt/Casos GT v1.xlsx")
    urllib.request.urlretrieve(url, "datacovidgt/LOCALIZACION GT.xlsx")

    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!62459&parId=9F999E057AD8C646!62453&authkey=!AkePW7UW1KXQkMM&app=Excel"
    #urllib.request.urlretrieve(url, "../Datos_Guatemala/datacovidgt/LOCALIZACION GT.xlsx")
    urllib.request.urlretrieve(url, "datacovidgt/Casos GT v1.xlsx")
    #Salud GT.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!62481&parId=9F999E057AD8C646!62453&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "datacovidgt/Salud GT.xlsx")
    #Farmacias GT.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!62478&parId=9F999E057AD8C646!62453&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "datacovidgt/Farmacias GT.xlsx")
    #DATOS RRSS GT.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!62465&parId=9F999E057AD8C646!62453&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "datacovidgt/DATOS RRSS GT.xlsx")
    #Condición_Pacientes GT.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!62474&parId=9F999E057AD8C646!62453&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "datacovidgt/Condición_Pacientes GT.xlsx")
    #Alimentación GT.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!62480&parId=9F999E057AD8C646!62453&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "datacovidgt/Alimentación GT.xlsx")
    #00 DATACOVID_GT_CUARENTENA.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!62485&parId=9F999E057AD8C646!62453&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "datacovidgt/00 DATACOVID_GT_CUARENTENA.xlsx")
    #00 DATACOVID Trabajo_GT.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!62473&parId=9F999E057AD8C646!62453&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "datacovidgt/00 DATACOVID Trabajo_GT.xlsx")
    #ECONOMICOS HN Y GT.xlsx
    url = "https://onedrive.live.com/download?cid=9f999e057ad8c646&page=view&resid=9F999E057AD8C646!62665&parId=9F999E057AD8C646!62453&authkey=!AkePW7UW1KXQkMM&app=Excel"
    urllib.request.urlretrieve(url, "datacovidgt/ECONOMICOS HN Y GT.xlsx")

    return 

#************************************Actualizar Datos de la organizacion*******************************************

if __name__ == '__main__':
    print('Iniciado proceso...')
    UpdateDatabase()
    print('Proceso finalizado.')