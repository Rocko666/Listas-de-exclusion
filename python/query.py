
def q_obtener_numeros_lista_negra():
    qry = """
    SELECT 
        fecha_registro
        ,num_celular
        ,tipo_lista
        ,fecha_eliminacion
    FROM (
    SELECT 
        fecha_registro
        ,num_celular
        ,"Lista Negra" AS tipo_lista
        ,fecha_eliminacion
        , ROW_NUMBER() OVER(PARTITION BY num_celular ORDER BY fecha_registro DESC) AS rn
    FROM db_campana.otc_t_lista_negra_sms_hist
    WHERE num_celular like ('9%')) AS xx
    WHERE xx.rn =1
    """
    return qry

def q_obtener_identificadores_base_consentimiento(FECHA_EJECUCION):
    qry = """
    SELECT DISTINCT
        a.FECHAINICIO AS fecha_registro
        ,p.telefono AS num_celular
        ,UPPER(a.MARCA) AS marca
        ,a.DOCUMENTO AS identificador
        ,"Base Consentimiento" AS tipo_lista
    FROM db_consentimiento.LOPDP_T_GESTIONPREFERENCIA a
    INNER JOIN db_consentimiento.LOPDP_T_CATSUBPROPOSITO b
        on a.SUBPROPOSITOTRATAMIENTO = b.ID_SUBPROPOSITO 
        and UPPER(a.MARCA)=UPPER(b.MARCA) 
        AND b.FECHAFIN IS NULL
    INNER JOIN db_cs_altas.otc_t_prq_glb_bi p 
        on a.DOCUMENTO= p.ruc 
        and p.fecha_proceso={FECHA_EJECUCION}
    WHERE
        a.FECHAFIN IS NULL
        AND (ID_SUBPROPOSITO like 'CC%' --subpropositos de comunicacion comercial solicitada ono y relacionada o no con los servicios Otecel
        or ID_SUBPROPOSITO like 'C-%')
        AND a.TOGGLE in('NO','1')
    """.format(FECHA_EJECUCION=FECHA_EJECUCION)
    return qry

def q_obtener_numeros_movi_parque(FECHA_EJECUCION):
    qry = """
    SELECT 
        num_celular
        ,marca
        ,identificador
    FROM(
    SELECT 
        num_telefonico AS num_celular
        ,UPPER(marca) AS marca
        ,documento_cliente AS identificador
        , ROW_NUMBER() OVER(PARTITION BY num_telefonico ORDER BY fecha_alta DESC) AS rn
    FROM db_cs_altas.otc_t_nc_movi_parque_v1
    WHERE fecha_proceso={FECHA_EJECUCION}) xx
    WHERE xx.rn=1
    """.format(FECHA_EJECUCION=FECHA_EJECUCION)
    return qry

def q_obtener_marca_y_documento_cliente(vSChema):
    qry = """
    SELECT 
        fecha_registro
        ,num_celular
        ,CASE WHEN marca='TELEFONICA' THEN 'MOVISTAR' ELSE marca END AS marca
        ,identificador
        ,tipo_lista
    FROM (
    SELECT ln.fecha_registro,
        ln.num_celular,
        mp.marca,
        mp.identificador,
        ln.tipo_lista
    FROM {vSChema}.numeros_lista_negra_exc ln
    INNER JOIN {vSChema}.numeros_movi_parque_exc mp
        ON ln.num_celular = mp.num_celular) AS f
    """.format(vSChema=vSChema)
    return qry
    
def q_registros_repetidos(vSChema):
    qry = """
    SELECT 
        a.num_celular
    FROM {vSChema}.numeros_lista_negra_final_exc a
    INNER JOIN {vSChema}.identificadores_base_consentimiento_exc b
        ON a.num_celular = b.num_celular
    """.format(vSChema=vSChema)
    return qry

def q_lista_negra_sr(vSChema):
    qry = """
    SELECT 
        fecha_registro
        , num_celular
        , marca
        , identificador
        , tipo_lista
    FROM {vSChema}.numeros_lista_negra_final_exc a
    WHERE num_celular 
    NOT IN (SELECT num_celular FROM registros_repetidos)
    """.format(vSChema=vSChema)
    return qry