# -*- coding: utf-8 -*-
"""
Created on Tue Jul  5 13:14:05 2022

@author: B39670 - Martin Ordoñez

"""
from datetime import datetime 
import pandas as pd
# import numpy as np
import sqlalchemy as db 
import pyodbc

#---------------------------------------------- ENGINE ---------------------------------------------

SERVER_NAME = 'CFERNANDEZSAXP\SERVIDORRETAIL' 
DATABASE_NAME = 'BDTCADQ' 
DRIVER = 'SQL Server' 
USERNAME='usuario_dn'
PASSWORD='escritura'


ENGINE = db.create_engine('mssql+pyodbc://'+USERNAME+':'+PASSWORD+'@' + SERVER_NAME + '/' + DATABASE_NAME
                          + '?driver='+DRIVER) 
START = datetime.now()

PERIODO='202212' #CAMBIAR MENSUALMENTE
PERIODO2='Diciembre'#'Septiembre'  #CAMBIAR MENSUALMENTE

#DECORADORES

def greet(name):
    if name>1000000: #000
        return "{:,.0f}".format(name/1000)
    elif name>10000: #000
        return "{:,.1f}".format(name/1000)
    else: 
        return "{:,.2f}".format(name/1000)


def image(var):
    if var<0.8:
        return('<img src="https://i.imgur.com/kXG5WcL.png">')#https://i.imgur.com/IpkA0VE.png
    elif var<1:
        return('<img src="https://i.imgur.com/osr3btT.png">' )# #https://i.imgur.com/C6bbrYm.png
    elif var<20:
        return('<img src="https://i.imgur.com/JKkt1FZ.png">')#https://i.imgur.com/RmlPCbk.png
    else:
        return('')
    

CABECERA= '''       
        <br><table style="width: 90%; border-collapse: collapse; margin-left: auto; margin-right: auto;text-align: center;" border="0">
        <tbody>
        <tr style="height:8px;background-color:#f2f2f2;border-radius:20px;">
        <th style="width: 9.5%; font-family:Arial;font-size:12px;background-color:white;color:white" ;" colspan="2"></th>
        <th style="width: 7.5%; font-family:Arial;font-size:12px;background-color:#05334D;color:white;border-radius:0px;border-right: solid 1px white" ;" colspan="3">Acumulado Mensual</th>
        <th style="width: 7.5%; font-family:Arial;font-size:12px;background-color:#05334D;color:white;border-radius:0px" ;" colspan="3">Acumulado Anual</th>
        <th style="width: 7.5%; font-family:Arial;font-size:12px;background-color:white;color:white;border-radius:0px" ;" colspan="1"></th>
        <tr style="height:18px;">
        <th style="width: 7.5%; font-family:Arial;font-size:12px;background-color:#05334D;color:white">Canal</th>
        <th style="width: 9.5%; font-family:Arial;font-size:12px;background-color:#05334D;color:white;border-right: solid 1px white">Subcanal</th>
        <th style="width: 7.5%; font-family:Arial;font-size:12px;background-color:#05334D;color:white">Real</th>
        <th style="width: 7.5%; font-family:Arial;font-size:12px;background-color:#05334D;color:white">Meta</th>
        <th style="width: 7.5%; font-family:Arial;font-size:12px;background-color:#05334D;color:white;border-right: solid 1px white">%Avance</th>
        <th style="width: 7.5%; font-family:Arial;font-size:12px;background-color:#05334D;color:white">Real</th>
        <th style="width: 7.5%; font-family:Arial;font-size:12px;background-color:#05334D;color:white">Meta</th>
        <th style="width: 7.5%; font-family:Arial;font-size:12px;background-color:#05334D;color:white;border-right: solid 1px white">%Avance</th>
        <th style="width: 7.5%; font-family:Arial;font-size:12px;background-color:#05334D;color:white">Fecha Actualización</th>
        </tr>
        <tr style="height:10px;"></tr>
        '''

#-----------------------------------------------------------
#----------------------------------------------------------- DATAFRAME



DATAFRAME = pd.read_sql_query('''
                              SELECT *
                              from SEGUIMIENTO_CANAL WHERE PERIODO='''+PERIODO+'''OR
							  (LEFT(CONVERT(VARCHAR,FECHA,112),6) ='''+PERIODO+''' 
                              AND 'FFVV HIPOTECARIO DIF' NOT IN 
							  (SELECT DISTINCT SUBCANAL FROM SEGUIMIENTO_CANAL WHERE 
							  PERIODO='''+PERIODO+''') AND SUBCANAL !='TOTAL')
                              ORDER BY Subcanal ASC;
                              '''
                              ,ENGINE)

DATAFRAME['SUBCANAL'].mask(DATAFRAME['PERIODO']==int(PERIODO)-1,'ANT FFVV HIPOTECARIO DIF*' , inplace=True)
DATAFRAME.sort_values(['UNIDAD','CANAL','SUBCANAL',],inplace=True)

# --AND UNIDAD='CANTIDAD'
CANALES =['GCVE','TIENDAS','DIGITAL'] #
DATAFRAME[['SUBCANAL']] = DATAFRAME[['SUBCANAL']].astype(str).apply(lambda col: col.str.title()) 

#COLOCACIONES CANTIDAD

df_temp=DATAFRAME[DATAFRAME['UNIDAD']=='CANTIDAD']

# lista=df_temp['CANAL'].unique().tolist()


BODY_1='''<!DOCTYPE html><html><body><p style="color:black;font-family:Arial;font-size:12px;">
        Equipo,
        <br>Alcanzamos el resumen de los canales a la fecha de corte.</p>
        <p style="font-weight: bold;color:black;font-family:Arial;font-size:12px;">Enlace:
        &nbsp;<a href="https://app.powerbi.com/links/8muChroX9U?ctid=360bc517-7aac-4c17-9907-4c5f13a12289&pbi_source=linkShare">Boletín_Seguimiento_Canales</a>
        <br>
        <br>► Colocaciones en Cantidad (Miles) - '''+PERIODO2+'''</p>
       '''


x=''

for j in CANALES:
    for i in range(len(df_temp[df_temp.CANAL==j])):
        df=df_temp[df_temp.CANAL==j]
        df.reset_index(drop=True, inplace=True)
        if df.loc[i,'SUBCANAL']=='Total':
            Format='<td style="background-color:#DFDFDF;font-family:Arial;font-size:12px;font-weight: bold;">'
            Format2='<td style="background-color:#DFDFDF;font-family:Arial;font-size:12px;font-weight: bold;border-right: solid 1px white;">'
        else:
            Format='<td style="font-family:Arial;font-size:12px;">'
            Format2='<td style=";border-right: solid 1px white;font-family:Arial;font-size:12px;">'
        if i ==0:
            x=x+'<tr style="color:black;font-family:Arial;font-size:12px;background-color:#f2f2f2;border-radius:20px;"><td style="font-weight: bold;font-family:Arial;font-size:12px;color:black;" rowspan="'+str(int(len(df[df.CANAL==j])+0))+'"><p>'+j+'</p></td>'
        else:
            x=x+'<tr style="color:black;background-color:#f2f2f2;font-family:Arial;font-size:12px;">'
        
        x=x+Format2+df.loc[i,'SUBCANAL']+'</TD>'
        x=x+Format+ greet(df.loc[i,'REAL']) +'</TD>'
        x=x+Format+ greet(df.loc[i,'META'])+'</TD>'
        x=x+Format2+image(df.loc[i,'AVANCE_META_TIMING'])+" " +"{:.1%}".format(df.loc[i,'AVANCE_META']).rjust(4,'Δ')+'</TD>'
        x=x+Format+ greet(df.loc[i,'ACUM_REAL'])+'</TD>'
        x=x+Format+ greet(df.loc[i,'ACUM_META'])+'</TD>'
        x=x+Format2+image(df.loc[i,'AVANCE_ACUM_META_TIMING'])+" "+"{:.1%}".format(df.loc[i,'AVANCE_ACUM_META']).rjust(4,'Δ')+'</TD>'
        if df.loc[i,'SUBCANAL']=='Total':
            x=x+Format+'</TD>'
 
        else:
            x=x+Format+str(df.loc[i,'FECHA'])+'</TD>'
    if j=='DIGITAL':
        pass
    x=x+'<tr style="height:10px;"></tr>'
    
#CORREO 1 
x=x+'</body></table></html>'
x=x+'<p style="font-family:Arial;font-size:12px;">• Se está considerando la venta contextual por BT, TLV, FFVV y Telemarketing'
x=x+'<br>• Se incluye (CS - AP, CTS, IL, UPG , PA, Seguros, TC Activas, TC Adicionales)</p>'

x=BODY_1+CABECERA+x    

#COLOCACIONES MONTO

df_temp=DATAFRAME[DATAFRAME['UNIDAD']=='MONTO']


BODY_2='''
        <p style="font-family:Arial;font-size:12px;font-weight: bold;color:black;">► Colocaciones en Monto (Miles) - '''+PERIODO2+'''</p>
       '''


y=''    
for j in CANALES:
    for i in range(len(df_temp[df_temp.CANAL==j])):
        df=df_temp[df_temp.CANAL==j]
        df.reset_index(drop=True, inplace=True)
        if df.loc[i,'SUBCANAL']=='Total' or df.loc[i,'SUBCANAL']=='Ant Ffvv Hipotecario Dif*':
            Format='<td style="background-color:#DFDFDF;font-weight: bold;font-family:Arial;font-size:12px;">'
            Format2='<td style="background-color:#DFDFDF;;font-weight: bold;border-right: solid 1px white;">'
        else:
            Format='<td style="font-family:Arial;font-size:12px;">'
            Format2='<td style="border-right: solid 1px white;font-family:Arial;font-size:12px;">'
        if i ==0:
            y=y+'<tr style="background-color:#f2f2f2;font-family:Arial;font-size:12px;"><td style="font-family:Arial;font-size:12px;font-weight: bold;color:black;" rowspan="'+str(int(len(df[df.CANAL==j])+0))+'"><p>'+j+'</p></td>'
        else:
            y=y+'<tr style="background-color:#f2f2f2;font-family:Arial;font-size:12px;">'
  
        y=y+Format2+df.loc[i,'SUBCANAL']+'</TD>'
        y=y+Format+ greet(df.loc[i,'REAL']) +'</TD>'
        y=y+Format+ greet(df.loc[i,'META'])+'</TD>'
        y=y+Format2+image(df.loc[i,'AVANCE_META_TIMING'])+" "+ "{:.1%}".format(df.loc[i,'AVANCE_META']).rjust(4,'Δ')+'</TD>'
        y=y+Format+ greet(df.loc[i,'ACUM_REAL'])+'</TD>'
        y=y+Format+ greet(df.loc[i,'ACUM_META'])+'</TD>'
        y=y+Format2+image(df.loc[i,'AVANCE_ACUM_META_TIMING'])+" "+ "{:.1%}".format(df.loc[i,'AVANCE_ACUM_META']).rjust(4,'Δ')+'</TD>'
        if df.loc[i,'SUBCANAL']=='Total':
            y=y+Format+'</TD>'
        else:
            y=y+Format+str(df.loc[i,'FECHA'])+'</TD>'
    if j=='DIGITAL':
        pass
    y=y+'<tr style="height:10px;"></tr>'
        
    
y=y+'</body></table></html>'
y=y+'<p style="font-family:Arial;font-size:12px;">• Se está considerando la venta contextual por BT, TLV y FFVV'
y=y+'<br>• Hipotecario e Hipotecario Dif. presentan un timing diferenciado' 
y=y+'<br>• Hipotecario Diferenciado* presenta un corte diferente por lo que se muestra la gestión anterior' 
y=y+'<br>• Se incluye (EC, PPE, CD, Convenios, Hipotecario y CS Abonos)</p>'      
y=BODY_2+CABECERA+y    
    
dff=DATAFRAME 
# # #LIMPIEZA FINAL   
 
html=x+y    
html=html.replace('Ffvv Cs','Cuenta Sueldo')   
html=html.replace('nan%','')      
html=html.replace('nan','')   
html=html.replace('Ffvv ','')
html=html.replace('FFVV HIPOTECARIO','HIPOTECARIO')  
html=html.replace('GCVE','Gerencia Central de Ventas y Experiencia')  
html=html.replace('Tlv','TLV')  
html=html.replace('Gdp','GDP')
html=html.replace('TIENDAS','Tiendas')
html=html.replace('DIGITAL','Digital')  
html=html.replace('Div','División') 
html=html.replace('None','-')   
html=html.replace('Δ','<span>&nbsp;</span>')   
html=html + '<p style="font-family:Arial;font-weight: bold;font-size:12px">Saludos'
html=html + '<br>Desarrollo de Negocios</p>'   
html=html + '<img src="https://i.imgur.com/gqh4ipx.png"/>'
    
print('Se armo el HTML')

DESTINATARIOS='mordoez@intercorp.com.pe'
# DESTINATARIOS='jvascones@intercorp.com.pe; carnao@intercorp.com.pe; ymarquezado@intercorp.com.pe;jrivadeneyra@intercorp.com.pe; ggiraldo@intercorp.com.pe;myaya@intercorp.com.pe; mmelgard@intercorp.com.pe; pjimenez@intercorp.com.pe;awoodman@intercorp.com.pe;lcubas@intercorp.com.pe;dvegasl@intercorp.com.pe'
COPIAS='mordoez@intercorp.com.pe'
# COPIASOCULTAS='tmancilla@intercorp.com.pe;vortegaa@intercorp.com.pe;jcollantesgo@intercorp.com.pe;jromo@intercorp.com.pe;mordoez@intercorp.com.pe;ntocas@intercorp.com.pe' #'mordoez@intercorp.com.pe'
# COPIAS='asalazarr@intercorp.com.pe;tmancilla@intercorp.com.pe;vortegaa@intercorp.com.pe;mordoez@intercorp.com.pe;jcollantesgo@intercorp.com.pe;jromo@intercorp.com.pe;ntocas@intercorp.com.pe'

SUBJECT='Alerta Canales VP Retail ' + PERIODO2
try:
      cnxn = pyodbc.connect(r'Driver=SQL Server;Server=CFERNANDEZSAXP\SERVIDORRETAIL;Database=BDTCADQ;UID=usuario_dn;PWD=escritura;')
      cursor_2 = cnxn.cursor()
      cnxn.autocommit = True
      cursor_2.execute("exec [dbo].[GCVE_ALERTA_GENERICA_CC] ?,?,?,?", html, SUBJECT , DESTINATARIOS,COPIAS )
      print('Enviado')
except Exception: # as e:
      print('Error al conectarse al Fernandez')

print('Tiempo de Ejecucion: ' ,datetime.now() - START)
# print(html)


