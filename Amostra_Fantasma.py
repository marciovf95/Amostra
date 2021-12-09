# coding=UTF-8
import os
import urllib.request as urllib2
import cx_Oracle
import gc
os.environ["NLS_LANG"] = ".UTF8"

BI_CODIGOS_AMOSTRA = (""" SELECT C594 COD_AMOSTRA FROM PADOBI_M21 WHERE C619 = 'ATRASADO' AND (C2081 <> '' OR C2081 IS NOT NULL) AND C10547 LIKE '%LD1:%' AND C10547 LIKE '%COR_CEPO%' GROUP BY C594 """)

FO_CODIGOS_FANTASMAS = ("""
    SELECT
            PAI.COD_ITEM PAI,
            FILHO.COD_ITEM,
            FILHO.DESC_TECNICA,
            ALMO.COD_ALMOX,
            TP_ESTRUTURA,
              TFUNCIONARIOS.COD_FUNC||' - '||tfuncionarios.NOME PLANEJADOR
        FROM
            TCAD_EST_ITE EST,
            TITENS PAI,
            TITENS FILHO,
            TALMOXARIFADOS ALMO,
            titens_planejamento pla,
            TITENS_PLAN_FUNC plafun,
            tfuncionarios
        WHERE
            EST.PAI_ID = PAI.ID
            AND TRUNC(SYSDATE) BETWEEN DT_INI AND DT_FIM
            AND plafun.itpl_id(+) = pla.id
            AND plafun.func_id = tfuncionarios.id(+)
            AND ALMO.ID = ALMOX_ID
            AND pla.cod_item = filho.cod_item
            AND PAI.SIT = 1
            AND FILHO.SIT = 1
            AND TP_ESTRUTURA IN ('N', 'F')
            AND EST.FILHO_ID = FILHO.ID
            AND PAI.COD_ITEM IN (?) 
            AND FILHO.DESC_TECNICA LIKE '%CEPO%'
            """)

BI_PEDIDOS_AMOSTRA_MASCARA = """
SELECT
  C594 COD_AMOSTRA,
  C573 PEDIDO,
  C7725 MASCARA
FROM
  PADOBI_M21
WHERE
  C619 = 'ATRASADO'
  AND (C2081 <> '' OR C2081 IS NOT NULL)
GROUP BY
  C594,
  C573,
  C7725
ORDER BY
  C594 DESC"""

FO_ITEMUM = """
  SELECT TEMPRESAS.COD_EMP COD_EMP_NUMBER,
      TITENS.COD_ITEM COD_ITEM_PAI_VARCHAR2,
      TITENS.DESC_TECNICA DESC_TECNICA_PAI_VARCHAR2,
      TCAD_EST_ITE.SEQ_ORD SEQ_ORD_NUMBER,
      TITENS1.COD_ITEM COD_ITEM_FILHO_VARCHAR2,
      TITENS1.DESC_TECNICA DESC_TECNICA_FILHO_VARCHAR2,
      TEST_CONF_VAR.TCARAC_ID TCARAC_ID_NUMBER,
      TEST_CONF_VAR.COD_CAR COD_CAR_VARCHAR2,
      TVARS_EST_CONF.TVAR_ID TVAR_ID_NUMBER,
      TVARIAVEIS.DESCRICAO DESCRICAO_VARCHAR2,
      TVARIAVEIS.MNEMONICO
  FROM TCAD_EST_ITE TCAD_EST_ITE,
      TEMPRESAS TEMPRESAS,
      TITENS TITENS,
      TITENS TITENS1,
      TEST_CONF_VAR TEST_CONF_VAR,
      TITENS_CAR TITENS_CAR,
      TVARS_EST_CONF TVARS_EST_CONF,
      TVARIAVEIS TVARIAVEIS 
WHERE TCAD_EST_ITE.ID = TEST_CONF_VAR.CAD_EST_IT_ID(+)
  AND TEMPRESAS.ID = TCAD_EST_ITE.EMPR_ID
  AND TITENS.ID = TCAD_EST_ITE.PAI_ID
  AND TITENS1.ID = TCAD_EST_ITE.FILHO_ID
  AND TEST_CONF_VAR.ID = TVARS_EST_CONF.EST_CF_VAR_ID
  AND TITENS_CAR.ID = TEST_CONF_VAR.TITEM_CAR_ID
  AND TVARIAVEIS.ID(+) = TVARS_EST_CONF.TVAR_ID   
  AND (TEMPRESAS.COD_EMP = 01)
  AND (TITENS.COD_ITEM = CODIGOITEM)
  AND (( TCAD_EST_ITE.TP_ESTRUTURA in ('N','F')))
  AND TITENS1.DESC_TECNICA LIKE '%CORAMOSTRA%'
  AND TVARIAVEIS.MNEMONICO LIKE 'CODIGOAMOSTRA%' """

Pergunta_Resposta_Amostra = """
  SELECT LISTAGG(TVARIAVEIS.DESCRICAO ,',')WITHIN GROUP (ORDER BY TVARIAVEIS.ID )  
         --TVARIAVEIS.DESCRICAO
  FROM TMASC_ITEM TMASC_ITEM,
       TCONFIG_ITENS TCONFIG_ITENS,
       TCARACTERISTICAS TCARACTERISTICAS,
       TVARIAVEIS TVARIAVEIS
 WHERE TMASC_ITEM.ID = TCONFIG_ITENS.TMASC_ITEM_ID
   AND TCARACTERISTICAS.ID = TCONFIG_ITENS.TCARAC_ID
   AND TVARIAVEIS.ID = TCONFIG_ITENS.TVAR_ID
   AND (TMASC_ITEM.ID = ?)
   AND (( TCARACTERISTICAS.ID in (1548,1586))) """

CODIGO_COMPRA ="""
   WITH temp AS 
   (SELECT TITENS.COD_ITEM COD_ITEM_PAI_VARCHAR2,
       TITENS.DESC_TECNICA DESC_PAI_VARCHAR2,
       TCAD_EST_ITE.SEQ_ORD SEQ_ORD_NUMBER,
       TITENS1.COD_ITEM COD_ITEM_FILHO_VARCHAR2,
       TITENS1.DESC_TECNICA DESC_FILHO_VARCHAR2,
       TCARACTERISTICAS.COD_CAR COD_CAR_VARCHAR2,
       TEST_CONF_VAR.OPERADOR OPERADOR_VARCHAR2,
       TVARIAVEIS.DESCRICAO VALIDADOR_VARCHAR2,
       TVARIAVEIS.MNEMONICO MNEMONICO_VARCHAR2
  FROM TCAD_EST_ITE TCAD_EST_ITE,
       TITENS TITENS,
       TITENS TITENS1,
       TEST_CONF_VAR TEST_CONF_VAR,
       TCARACTERISTICAS TCARACTERISTICAS,
       TVARS_EST_CONF TVARS_EST_CONF,
       TVARIAVEIS TVARIAVEIS
 WHERE TCAD_EST_ITE.ID = TEST_CONF_VAR.CAD_EST_IT_ID
   AND TITENS.ID = TCAD_EST_ITE.PAI_ID
   AND TITENS1.ID = TCAD_EST_ITE.FILHO_ID
   AND TEST_CONF_VAR.ID = TVARS_EST_CONF.EST_CF_VAR_ID
   AND TCARACTERISTICAS.ID = TEST_CONF_VAR.TCARAC_ID
   AND TVARIAVEIS.ID = TVARS_EST_CONF.TVAR_ID
   AND (TITENS.COD_ITEM = ?COD )
   AND (( TCARACTERISTICAS.COD_CAR in ('FECH_FILHO','COR DO CEPO')))
   AND (TEST_CONF_VAR.OPERADOR = '=')    
   AND TVARIAVEIS.DESCRICAO = '?COR'
   )
   SELECT DESCRICAO.COD_ITEM_FILHO_VARCHAR2,DESCRICAO.DESC_FILHO_VARCHAR2 FROM 
   (SELECT TITENS.COD_ITEM COD_ITEM_PAI_VARCHAR2,
       TITENS.DESC_TECNICA DESC_PAI_VARCHAR2,
       TCAD_EST_ITE.SEQ_ORD SEQ_ORD_NUMBER,
       TITENS1.COD_ITEM COD_ITEM_FILHO_VARCHAR2,
       TITENS1.DESC_TECNICA DESC_FILHO_VARCHAR2,
       TCARACTERISTICAS.COD_CAR COD_CAR_VARCHAR2,
       TEST_CONF_VAR.OPERADOR OPERADOR_VARCHAR2,
       TVARIAVEIS.DESCRICAO VALIDADOR_VARCHAR2,
       TVARIAVEIS.MNEMONICO MNEMONICO_VARCHAR2
  FROM TCAD_EST_ITE TCAD_EST_ITE,
       TITENS TITENS,
       TITENS TITENS1,
       TEST_CONF_VAR TEST_CONF_VAR,
       TCARACTERISTICAS TCARACTERISTICAS,
       TVARS_EST_CONF TVARS_EST_CONF,
       TVARIAVEIS TVARIAVEIS
 WHERE TCAD_EST_ITE.ID = TEST_CONF_VAR.CAD_EST_IT_ID
   AND TITENS.ID = TCAD_EST_ITE.PAI_ID
   AND TITENS1.ID = TCAD_EST_ITE.FILHO_ID
   AND TEST_CONF_VAR.ID = TVARS_EST_CONF.EST_CF_VAR_ID
   AND TCARACTERISTICAS.ID = TEST_CONF_VAR.TCARAC_ID
   AND TVARIAVEIS.ID = TVARS_EST_CONF.TVAR_ID
   AND (TITENS.COD_ITEM = ?COD )
   AND (( TCARACTERISTICAS.COD_CAR in ('FECH_FILHO','COR DO CEPO')))
   AND (TEST_CONF_VAR.OPERADOR = '=')   
   AND TVARIAVEIS.DESCRICAO like 'F?%'
   )
   DESCRICAO,temp
   WHERE DESCRICAO.COD_ITEM_FILHO_VARCHAR2 = TEMP.COD_ITEM_FILHO_VARCHAR2 AND rownum =1
   """
# SQLIN=""" INSERT INTO PADOBI_M198 (C10915,C10916,C10917,C10918,C10919,C10920,C10921,C10922,C10923,C10924) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10) """
SQLIN=""" INSERT INTO PADOBI_M198 (C10915,C10916,C10917,C10918,C10919,C10920,C10921,C10922) VALUES (:1, :2, :3, :4, :5, :6, :7, :8 ) """
SQLDEL=""" DELETE FROM PADOBI_M198  """

def listToString(s):
    str1 = ""      
    for ele in s: 
        str1 += ele      
    
    return str1

try: 
  print("Conectando ao BI")
  connectionBI = cx_Oracle.connect('privado')
  print("Conectado ao ->BI<-")
  cursorBI = connectionBI.cursor()
  cursorBI.execute(BI_CODIGOS_AMOSTRA)
  
  Cod_Amostras = [] 
  for row in cursorBI:
    Cod_Amostras.extend(row)


  print("XxX Fechando BI XxX")
  cursorBI.close()  
  
  print("Conectando ao FOCCO")
  connectionFOCCO = cx_Oracle.connect('privado')
  print("Conectado ao ->FOCCO<-")

  cursorFOCCO = connectionFOCCO.cursor()
  cursorFOCCO.execute(FO_CODIGOS_FANTASMAS.replace("?",(','.join(Cod_Amostras))))
  Agrupadores = []  
  for row in cursorFOCCO:    
    Agrupadores.append(row)


  Tem_Fantasma = []
  Sem_Fantasma = []
  for row in Agrupadores:    
    if "AGRUPADOR CEPO" in row[2]:
      if  row[4] == "F":
        Tem_Fantasma.append(list(row))
      else:
        Sem_Fantasma.append(list(row))

  codigoconsulta = []
  temp = []
  
  if(len(Tem_Fantasma))>=1:
    for rowlvl2 in Tem_Fantasma: 
      temp = []      
      x = list(rowlvl2)
      x.append(rowlvl2[0])
      temp.append(rowlvl2[0])
      temp.append(rowlvl2[1])
      codigoconsulta.append(temp)      

  print("XxX Fechando FOCCO XxX ")
  cursorFOCCO.close()  

  print("Conectado ao ->BI<-")
  cursorBI = connectionBI.cursor()
  cursorBI.execute(BI_PEDIDOS_AMOSTRA_MASCARA)
  

  Pedidos_pendentes = []
  for row in cursorBI.fetchall():
    Pedidos_pendentes.append(list(row)) 
    
  

  i=0
  Estrutura = []
  for x in range(len(Pedidos_pendentes)):
    for row in Tem_Fantasma: 
      if Pedidos_pendentes[x][0] == row[0]:  
        Estrutura.append(Pedidos_pendentes[x])       
        Estrutura[i].append(str(row[1]))         
        i+=1 
    
    
  tratamento = []
  print("Conectando ao FOCCO")
  connectionFOCCO = cx_Oracle.connect('privado')
  print("Conectado ao ->FOCCO<-")
  dct_Resposta = {}
  resposta = []
  temp = []
  cursorFOCCO = connectionFOCCO.cursor()
  print("Montar Dicionario de Estruturas")
  
  
  i=0
  for row in Estrutura: 
    cursorFOCCO.execute(Pergunta_Resposta_Amostra.replace("?",row[2]))
    for rowin in cursorFOCCO:
      if rowin[0] is not None: 
        tratamento.append(row)
        tratamento[i].append(listToString(list(rowin)))
        i+=1

  
 
  LIMP = []
  final = []

  for row in Estrutura:
    linha = (row) 
    LIMP = []       
    LIMP.append(linha[1])
    LIMP.append(linha[0])
    LIMP.append(linha[2])    
    LIMP.append(linha[3])
    marcio = linha[4].split(",")
    LIMP.append(marcio[0])
    LIMP.append(marcio[1])
    final.append(LIMP)
  

  
  print("Final Tratamento")
  linha = 0
  Estrutura_pre_inser = []
  for row in final:    
    if len(row) == 6:  
      # if row[0] == '400999':  
      #   print(row)
      #   print("Pedido ->>>>>>>>>>>>>>> 400999")
      tmp = CODIGO_COMPRA.replace("?COD",row[3])
      tmp = tmp.replace("?COR",row[4])
      tmp = tmp.replace("F?",row[5][0:8])
      listapre=[]
      cursorFOCCO.execute(tmp)
      for roww in cursorFOCCO:
        listapre.append(row[0])
        listapre.append(row[2])        
        listapre.append(row[1])        
        listapre.append(row[3])
        listapre.append(row[4])
        listapre.append(row[5])
        listapre.append(roww[0])
        listapre.append(roww[1])
        Estrutura_pre_inser.append(listapre)
        linha += 1

  

   
  print("XxX Fechando FOCCO XxX ")
  cursorFOCCO.close()  

  print("Conectado ao ->BI<- para Inserir")
  cursorBI = connectionBI.cursor()
  rows = []
  i=0
  print("Deletar tabela Para Insert")
  cursorBI.execute(SQLDEL)
  print("Setar Tamanho para o MANY do cxOracle")

  cursorBI.setinputsizes(cx_Oracle.STRING, cx_Oracle.STRING, cx_Oracle.STRING, cx_Oracle.STRING, cx_Oracle.STRING, cx_Oracle.STRING ,cx_Oracle.STRING, cx_Oracle.STRING)#, cx_Oracle.STRING,cx_Oracle.NUMBER)
  

  for row in Estrutura_pre_inser:    
    rows.append(row)
    i += 1 
    if i%1000 == 0:
      # print(rows)
      cursorBI.prepare(SQLIN)      
      cursorBI.executemany(None, rows)
      del rows
      gc.collect()
      rows = []
      print("Inserido "+str(i))  
  
  cursorBI.prepare(SQLIN)  
  cursorBI.executemany(None, rows)
  connectionBI.commit()
  cursorBI.close()
  
  print("Inserido OLHA O BIIIIII")
  print("Criando cache.")

  tempo = urllib2.urlopen("privado").read() 
  print("Importacao realizada, cache criado em "+str(tempo.decode("utf-8"))+".")
  
except OSError as err:
  print("OS error: {0}".format(err))
  print("EROOOOOOOO")