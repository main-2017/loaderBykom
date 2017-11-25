import re

editFile = open("m_telefonos.sql", "r", encoding='UTF8')
for line in editFile.readlines():
	posCadena = line.split(",")
	tel_iidcuenta = posCadena[0].strip("(")
	tel_iid = posCadena[1]
	tel_cnombre = posCadena[2]
	tel_cobservacion = posCadena[3]
	tel_ctelefono = posCadena[4]
	
	print("Cuenta: ",tel_iidcuenta ,"| Tel ID: ",tel_iid,"| Nombre: ",tel_cnombre , "| Observacion: ", tel_cobservacion, "| Telefono: ", tel_ctelefono)