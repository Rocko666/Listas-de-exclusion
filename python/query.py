
def q_obtener_numeros_lista_negra():
    qry = """
    SELECT fecha_registro,
        num_celular,
        "Lista Negra" AS tipo_lista,
        fecha_eliminacion
    FROM db_campana.otc_t_lista_negra_sms_hist
    WHERE num_celular like ('9%')
    """
    return qry


def q_obtener_identificadores_base_consentimiento():
    qry = """

SELECT FECHAINICIO AS fecha_registro,
NUMCELULAR AS num_celular,
UPPER(MARCA) AS marca,
DOCUMENTO AS identificador,
"Base Consentimiento" AS tipo_lista
FROM DB_CONSENTIMIENTO.LOPDP_T_GESTIONPREFERENCIA
WHERE NUMCELULAR LIKE ('9%')

    """
    return qry


def q_obtener_numeros_movi_parque(FECHA_EJECUCION):
    qry = """
    SELECT num_telefonico AS num_celular,
        UPPER(marca) AS marca,
        documento_cliente AS identificador
    FROM db_cs_altas.otc_t_nc_movi_parque_v1
    WHERE fecha_proceso={FECHA_EJECUCION}
    """.format(FECHA_EJECUCION=FECHA_EJECUCION)
    return qry


def q_obtener_marca_y_documento_cliente(vSChema):
    qry = """
    SELECT ln.fecha_registro,
        ln.num_celular,
        mp.marca,
        mp.identificador,
        ln.tipo_lista
    FROM {vSChema}.numeros_lista_negra_exc ln
    INNER JOIN {vSChema}.numeros_movi_parque_exc mp
        ON ln.num_celular = mp.num_celular
    """.format(vSChema=vSChema)
    return qry
    