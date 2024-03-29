# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import os
import argparse
from datetime import datetime, timedelta
from pyspark.sql.functions import col, lit, desc
from pyspark.sql import SparkSession
from query import *
sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *
from create import *


timestart = datetime.now()
vSStep = '[Paso 1]: Obteniendo parametros de la SHELL'
print(lne_dvs())
print(etq_info(vSStep))
try:
    ts_step = datetime.now()  

    parser = argparse.ArgumentParser()
    parser.add_argument('--vSEntidad', required=True, type=str, help='Entidad del proceso')
    parser.add_argument('--vSChema', required=True, type=str, help='')
    parser.add_argument('--vSChemaTmp', required=True, type=str, help='')
    parser.add_argument('--vFechaEje', required=True, type=str, help='')
    parser.add_argument('--vFechaEjeAnterior', required=True, type=str, help='')
    
    parametros = parser.parse_args()
    vSEntidad = parametros.vSEntidad
    vSChema = parametros.vSChema
    vSChemaTmp = parametros.vSChemaTmp
    vFechaEje = parametros.vFechaEje
    vFechaEjeAnterior = parametros.vFechaEjeAnterior

    print(lne_dvs())
    print(etq_info("Imprimiendo parametros..."))
    print(lne_dvs())
    print(etq_info(log_p_parametros("vSEntidad", str(vSEntidad))))
    print(etq_info(log_p_parametros("vSChema", str(vSChema))))
    print(etq_info(log_p_parametros("vSChemaTmp", str(vSChemaTmp))))
    print(etq_info(log_p_parametros("vFechaEje", str(vFechaEje))))
    print(etq_info(log_p_parametros("vFechaEjeAnterior", str(vFechaEjeAnterior))))

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))

print(lne_dvs())
vSStep = '[Paso 2]: Configuracion Spark Session'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()    

    spark = SparkSession. \
        builder. \
        config("spark.sql.caseSensitive", "true"). \
        enableHiveSupport(). \
        getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    app_id = spark._sc.applicationId

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [3]: Eliminar tablas'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()
    print(lne_dvs())

    spark.sql("DROP TABLE IF EXISTS {}.numeros_lista_negra_exc".format(vSChemaTmp))
    spark.sql("DROP TABLE IF EXISTS {}.identificadores_base_consentimiento_exc".format(vSChemaTmp))
    spark.sql("DROP TABLE IF EXISTS {}.numeros_movi_parque_exc".format(vSChemaTmp))
    spark.sql("DROP TABLE IF EXISTS {}.numeros_lista_negra_final_exc".format(vSChemaTmp))
    spark.sql("TRUNCATE TABLE {}.otc_t_exclusion_campanias".format(vSChema))

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(
        vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())


print(lne_dvs())
vSStep = 'Paso [4]: Obtener numeros de Lista Negra'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(lne_dvs())

    vSQL = q_obtener_numeros_lista_negra()    
    print(etq_sql(vSQL))
    lista_negra = spark.sql(vSQL)
    lista_negra = lista_negra.sort(desc("fecha_registro"))
    lista_negra_filtrada = lista_negra.dropDuplicates(["num_celular"])
    lista_negra_filtrada = lista_negra_filtrada.where(col("fecha_eliminacion").isNull())

    if lista_negra.limit(1).count <= 0:
        exit(etq_nodata(msg_e_df_nodata(str('lista_negra'))))
    else:
        vIRows = lista_negra.count()
        print(etq_info(msg_t_total_registros_obtenidos('lista_negra', str(vIRows))))

        lista_negra_filtrada.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.numeros_lista_negra_exc".format(vSChemaTmp))

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSChemaTmp+'.numeros_lista_negra_exc', vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())


print(lne_dvs())
vSStep = 'Paso [5]: Obtener identificadores de Base Consentimiento'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(lne_dvs())
    # se implementa este codigo para traer la ultima fecha y que el proceso
    # no traiga cero registros en base consentimiento
    fecha_resultante = datetime.strptime(vFechaEje, '%Y%m%d')
    fecha_resultante = fecha_resultante - timedelta(days=3)
    fecha_resultante = fecha_resultante.strftime('%Y%m%d')
    partitions = spark.sql("SHOW PARTITIONS "+"db_cs_altas.otc_t_prq_glb_bi")
    listpartitions = list(partitions.select('partition').toPandas()['partition'])
    cleanpartitions = [ i.split('=')[1] for i in listpartitions]
    cleanpartitions = [ i.split('/')[0] for i in cleanpartitions]
    filtered = [i for i in cleanpartitions if i >= str(fecha_resultante) and i <= str(vFechaEje)]
    print(filtered)
    fecha_particion=max(filtered)
    print(fecha_particion)
    vSQL = q_obtener_identificadores_base_consentimiento(fecha_particion)
    print(etq_sql(vSQL))
    base_consentimiento = spark.sql(vSQL)
    base_consentimiento = base_consentimiento.dropDuplicates(["num_celular", "identificador"])
    
    if base_consentimiento.limit(1).count <= 0:
        exit(etq_nodata(msg_e_df_nodata(str('base_consentimiento'))))
    else:
        vIRows = base_consentimiento.count()
        print(etq_info(msg_t_total_registros_obtenidos('base_consentimiento', str(vIRows))))
        base_consentimiento.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.identificadores_base_consentimiento_exc".format(vSChemaTmp))

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSChemaTmp+'.identificadores_base_consentimiento_exc', vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())


print(lne_dvs())
vSStep = 'Paso [6]: Obtener numeros de Movi Parque'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(lne_dvs())

    try:
        vSQL = q_obtener_numeros_movi_parque(vFechaEje)
        print(etq_sql(vSQL))
        movi_parque = spark.sql(vSQL)
    except Exception as e:
        exit(etq_error(msg_e_ejecucion(vSStep, str(e))))

        vSQL = q_obtener_numeros_movi_parque(vFechaEjeAnterior)
        print(etq_sql(vSQL))
        movi_parque = spark.sql(vSQL)    

    if movi_parque.limit(1).count <= 0:    
        exit(etq_nodata(msg_e_df_nodata(str('movi_parque'))))
    else:
        vIRows = movi_parque.count()
        print(etq_info(msg_t_total_registros_obtenidos('movi_parque', str(vIRows))))
        
        movi_parque.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.numeros_movi_parque_exc".format(vSChemaTmp))

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSChemaTmp+'.numeros_movi_parque_exc', vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())


print(lne_dvs())
vSStep = 'Paso [7]: Obtener marca e identificador de numeros de Lista Negra'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(lne_dvs())

    vSQL = q_obtener_marca_y_documento_cliente(vSChemaTmp)    
    print(etq_sql(vSQL))
    lista_negra = spark.sql(vSQL)
    lista_negra = lista_negra.dropDuplicates(["num_celular", "identificador"])

    if lista_negra.limit(1).count <= 0:    
        exit(etq_nodata(msg_e_df_nodata(str('lista_negra'))))
    else:
        vIRows = lista_negra.count()
        print(etq_info(msg_t_total_registros_obtenidos('lista_negra', str(vIRows))))
        
        lista_negra.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.numeros_lista_negra_final_exc".format(vSChemaTmp))

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSChemaTmp+'.numeros_lista_negra_final_exc', vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())


print(lne_dvs())
vSStep = 'Paso [8]: Unificar numeros de Lista Negra y Base Consentimiento'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()  
    print(lne_dvs())
    registros_repetidos = spark.sql(q_registros_repetidos(vSChemaTmp))
    registros_repetidos.createOrReplaceTempView("registros_repetidos")
    print("Registros en Lista Negra y Base Consentimiento:")
    registros_repetidos.show()
    lista_negra_sin_repetidos = spark.sql(q_lista_negra_sr(vSChemaTmp))
    exclusion_campanias = lista_negra_sin_repetidos.union(base_consentimiento)

    vFechaEjeFormat = datetime.strptime(str(vFechaEje), '%Y%m%d').strftime("%d/%m/%Y")
    exclusion_campanias = exclusion_campanias.withColumn("fecha_proceso", lit(vFechaEjeFormat))

    if exclusion_campanias.limit(1).count <= 0:    
        exit(etq_nodata(msg_e_df_nodata(str('exclusion_campanias'))))
    else:
        vIRows = exclusion_campanias.count()
        print(etq_info(msg_t_total_registros_obtenidos('exclusion_campanias', str(vIRows))))

        exclusion_campanias.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_exclusion_campanias".format(vSChema))

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSChema+'.otc_t_exclusion_campanias', vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))
print(lne_dvs())

print(lne_dvs())
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad, vle_duracion(timestart, timeend))))
print(lne_dvs())
