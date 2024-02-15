#########################################################################################################
# NOMBRE: SH_EXCLUSION_CAMPANIAS   		      							                                #
# DESCRIPCION:                                                                                          #
# AUTOR: Cesar Andrade - Softconsulting                            						                #
# FECHA CREACION: 2023-09-13   											                                #
# PARAMETROS DEL SHELL                            								                        #
#########################################################################################################
# MODIFICACIONES													                                    #
# FECHA  		AUTOR     		DESCRIPCION MOTIVO							                            #
# YYYY-MM-DD    NOMBRE			                                                     	  	            #
#########################################################################################################
set -e

#------------------------------------------------------
# PARAMETROS DE LA SHELL
#------------------------------------------------------
VAL_FECHA_EJECUCION=$1

ENTIDAD=XCLSNCMPNS0010
AMBIENTE=1 # AMBIENTE (1=produccion, 0=desarrollo)

if [ $AMBIENTE -gt 0 ]; then
    TABLA=params
else
    TABLA=params_des
fi

#------------------------------------------------------
# PARAMETROS DE LA TABLA PARAMS
#------------------------------------------------------
VAL_RUTA=$(mysql -N <<<"select valor from $TABLA where entidad = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';")
VAL_ESQUEMA=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA';")
VAL_ESQUEMA_TMP=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_TMP';")
VAL_COLA_EJECUCION=$(mysql -N <<<"select valor from $TABLA where entidad = '"$ENTIDAD"' AND parametro = 'VAL_COLA_EJECUCION';")

#------------------------------------------------------
# PARAMETROS SPARK
#------------------------------------------------------
VAL_RUTA_SPARK=$(mysql -N <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';")
VAL_MASTER=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';")
VAL_DRIVER_MEMORY=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';")
VAL_EXECUTOR_MEMORY=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';")
VAL_NUM_EXECUTORS=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';")
VAL_NUM_EXECUTORS_CORES=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS_CORES';")
VAL_KINIT=$(mysql -N <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';")
$VAL_KINIT

#------------------------------------------------------
# PARAMETROS CALCULADO
#------------------------------------------------------
ini_fecha=$(date '+%Y%m%d%H%M%S')
VAL_FECHA_EJECUCION_ANTERIOR=$(date '+%Y%m%d' -d "$VAL_FECHA_EJECUCION-1 day")
VAL_LOG=$VAL_RUTA/log/exclusion_campanias_$ini_fecha.log

#------------------------------------------------------
# VALIDACION DE PARAMETROS
#------------------------------------------------------
if [ -z "$VAL_FECHA_EJECUCION" ] ||
    [ -z "$VAL_RUTA" ] ||
    [ -z "$VAL_ESQUEMA" ] ||
    [ -z "$VAL_ESQUEMA_TMP" ] ||
    [ -z "$VAL_COLA_EJECUCION" ] ||
    [ -z "$VAL_RUTA_SPARK" ] ||
    [ -z "$VAL_MASTER" ] ||
    [ -z "$VAL_DRIVER_MEMORY" ] ||
    [ -z "$VAL_EXECUTOR_MEMORY" ] ||
    [ -z "$VAL_NUM_EXECUTORS" ] ||
    [ -z "$VAL_NUM_EXECUTORS_CORES" ] ||
    [ -z "$VAL_KINIT" ] ||
    [ -z "$VAL_LOG" ]; then
    echo " ERROR - uno de los parametros esta vacio o nulo"
    exit 1
fi

#------------------------------------------------------
# IMPRESION PARAMETROS
#------------------------------------------------------
echo "VAL_FECHA_EJECUCION: $VAL_FECHA_EJECUCION" >>$VAL_LOG
echo "VAL_RUTA: $VAL_RUTA" >>$VAL_LOG
echo "VAL_ESQUEMA: $VAL_ESQUEMA" >>$VAL_LOG
echo "VAL_ESQUEMA_TMP: $VAL_ESQUEMA_TMP" >>$VAL_LOG
echo "VAL_COLA_EJECUCION: $VAL_COLA_EJECUCION" >>$VAL_LOG
echo "VAL_RUTA_SPARK: $VAL_RUTA_SPARK" >>$VAL_LOG
echo "VAL_MASTER: $VAL_MASTER" >>$VAL_LOG
echo "VAL_DRIVER_MEMORY: $VAL_DRIVER_MEMORY" >>$VAL_LOG
echo "VAL_EXECUTOR_MEMORY: $VAL_EXECUTOR_MEMORY" >>$VAL_LOG
echo "VAL_NUM_EXECUTORS: $VAL_NUM_EXECUTORS" >>$VAL_LOG
echo "VAL_NUM_EXECUTORS_CORES: $VAL_NUM_EXECUTORS_CORES" >>$VAL_LOG
echo "VAL_KINIT: $VAL_KINIT" >>$VAL_LOG
echo "VAL_FECHA_EJECUCION_ANTERIOR: $VAL_FECHA_EJECUCION_ANTERIOR" >>$VAL_LOG
echo "VAL_LOG: $VAL_LOG" >>$VAL_LOG

#------------------------------------------------------
# GENERACION DE TABLA EXCLUSION CAMPANIAS
#------------------------------------------------------

$VAL_RUTA_SPARK \
    --name $ENTIDAD \
    --queue $VAL_COLA_EJECUCION \
    --master $VAL_MASTER \
    --driver-memory $VAL_DRIVER_MEMORY \
    --executor-memory $VAL_EXECUTOR_MEMORY \
    --num-executors $VAL_NUM_EXECUTORS \
    --executor-cores $VAL_NUM_EXECUTORS_CORES \
    $VAL_RUTA/python/exclusion_campanias.py \
    --vSEntidad=$ENTIDAD \
    --vSChema=$VAL_ESQUEMA \
    --vSChemaTmp=$VAL_ESQUEMA_TMP \
    --vFechaEje=$VAL_FECHA_EJECUCION \
    --vFechaEjeAnterior=$VAL_FECHA_EJECUCION_ANTERIOR 2>&1 &>> $VAL_LOG

echo "==== FIN PROCESO EXCLUSION CAMPANIAS ====" >>$VAL_LOG
