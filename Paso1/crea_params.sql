---PARAMETROS PARA LA ENTIDAD D_XCLSNCMPNS0010
DELETE FROM params_des WHERE ENTIDAD = 'D_XCLSNCMPNS0010';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_XCLSNCMPNS0010','PARAM1_FECHA_EJECUCION',"date_format(sysdate(),'%Y%m%d')",'0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_XCLSNCMPNS0010','SHELL','/home/nae108834/RGenerator/reportes/EXCLUSION_CAMPANIAS/bin/SH_EXCLUSION_CAMPANIAS.sh','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_XCLSNCMPNS0010','VAL_RUTA','/home/nae108834/RGenerator/reportes/EXCLUSION_CAMPANIAS','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_XCLSNCMPNS0010','VAL_ESQUEMA','db_desarrollo2021','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_XCLSNCMPNS0010','VAL_ESQUEMA_TMP','db_desarrollo2021','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_XCLSNCMPNS0010','VAL_COLA_EJECUCION','default','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_XCLSNCMPNS0010','VAL_MASTER','yarn','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_XCLSNCMPNS0010','VAL_DRIVER_MEMORY','8G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_XCLSNCMPNS0010','VAL_EXECUTOR_MEMORY','8G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_XCLSNCMPNS0010','VAL_NUM_EXECUTORS','4','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_XCLSNCMPNS0010','VAL_NUM_EXECUTORS_CORES','4','0','0');
SELECT * FROM params_des WHERE ENTIDAD = "D_XCLSNCMPNS0010";

