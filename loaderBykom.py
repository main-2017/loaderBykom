# Sistema para migracion de sistema de SoftGuard a Bykom Realizado por Agustin Ducca Pantaleon

#Importacion de modulos

import mysql.connector
import os
import time
import re

#Definicion de Funciones

def conexionDefault(bd):
	host = "localhost"
	user = "root"
	password = "VSCV.8.07.1988"
	database = mysql.connector.connect(host=host, user=user, passwd=password, db=bd)
	return database

def cursorMaker(database):
	cursor_db = database.cursor()
	return cursor_db

def obtenerListaInterior(cursorOrigen):
	tupla_orginal = cursorOrigen.fetchall()
	lista_original = list(tupla_orginal)
	lista_interna = []
	for lista_int in lista_original:
		lista_interna.append(list(lista_int))
	return lista_interna

def obtenerID_CL(id_Cuenta):
	query_cuentas_usuarios = "SELECT ORDER_ID FROM abmacodigos WHERE ID_CL = %d" %(id_Cuenta)
	cursor_destiny.execute(query_cuentas_usuarios)
	result = cursor_destiny.fetchone()
	if result != None:
		lista_result = list(result)
		str_result = str(lista_result[0])
	else:
		print("La cuenta no existe en tabla. Verifique el valor ingresado")
		str_result = "Inexistente"
	return str_result

def obtenerUsuariosporCuenta(id_Cuenta):
	order_rl = int(obtenerID_CL(id_Cuenta))
	query_usuarios_por_cuenta = "SELECT abrlusuarios.ORDER_ID, tlmapersonas.NOMBRE, tlmapersonas.NOMBRE_DOS, abrlusuarios.CLAVE, abrlusuarios.CONTRACLAVE FROM tlmapersonas, abrlusuarios WHERE abrlusuarios.CODIGO_ID = tlmapersonas.ORDER_ID AND abrlusuarios.ORDER_RL = %d" %(order_rl)
	cursor_destiny.execute(query_usuarios_por_cuenta)
	lista_interior_lobtenida = obtenerListaInterior(cursor_destiny)
	return lista_interior_lobtenida

def obtenerUsuariosSoftguard(idCuenta):
	query_origen = "SELECT * FROM m_usuarios WHERE usu_iidcuenta = %d" %(idCuenta)
	cursor_origin.execute(query_origen)
	usuarios_softguard = obtenerListaInterior(cursor_origin)
	return usuarios_softguard

def actualizacionCodigosBykom(nombre, apellido, ids):
	for n,a,i in zip(nombre,apellido,ids):
		update_nombre_destiny = "UPDATE abmacodigos SET NOMBRE = '%s', NOMBRE_DOS = '%s' WHERE ID_CL = %d" %(a,n,i)
		try:
			cursor_destiny.execute(update_nombre_destiny)
			db_destiny.commit()
			print("Cuenta Nº",i," Actualización realizada con éxito")
		except:
			db_destiny.rollback()
			print("Error durante la carga de datos en cuenta Nº",i)


def cargaMasivaPersonasBykom(nombre,apellido,ids):
	for n,a,i in zip(nombre,apellido,ids):
		insert_nombre_destiny = "INSERT INTO tlmapersonas(ORDER_ID, NOMBRE, NOMBRE_DOS) VALUES (%d,'%s','%s')" %(i, a, n)
		try:
			cursor_destiny.execute(insert_nombre_destiny)
			db_destiny.commit()
			print("_____________________________________ Proceso finalizado _____________________________________")
		except:
			db_destiny.rollback()
			os.system('color 04')
			print("Error durante la carga de datos")

def formatearDatosUsuariosSoftguard(cursor_origin, cursor_destiny):
	query_nombre_origin = "SELECT cue_iid, cue_cnombre FROM _datos ORDER BY cue_iid ASC"
	cursor_origin.execute(query_nombre_origin)
	lista_interior = obtenerListaInterior(cursor_origin)
	ids = []
	nombre_mix = []
	apellido = []
	nombre = []

	for index in lista_interior:
		ids.append(index[0])
		nombre_mix.append(index[1])

	for row in nombre_mix:
		meta_nombre = str(row)
		pos = meta_nombre.find(" ")
		apellido.append(meta_nombre[0:pos])
		nombre.append(meta_nombre[pos+1:])

	return nombre,apellido,ids

def cargaTelefonosSoftguard(loaderFile):
	point = ""
	for line in loaderFile.readlines():
		os.system("clear")
		posCadena = line.split(",")
		tel_iidcuenta = int(posCadena[0].strip("("))
		tel_iid = int(posCadena[1])
		tel_cnombre = posCadena[2]
		tel_cobservacion = posCadena[3]
		tel_ctelefono = posCadena[4]
		tel_ndiscado = 1
		tel_cpredigito = 0
		tel_cposdigito = 0
		tel_norden = 1
		query_insert_tel = "INSERT INTO m_telefonos(tel_iidcuenta, tel_iid, tel_cnombre, tel_cobservacion, tel_ctelefono, tel_ndiscado, tel_cpredigito, tel_cposdigito, tel_norden) VALUES (%d,%d,'%s','%s','%s',%d,%d,%d,%d)" %(tel_iidcuenta, tel_iid, tel_cnombre, tel_cobservacion, tel_ctelefono, tel_ndiscado, tel_cpredigito, tel_cposdigito, tel_norden)
		point += "."
		print("Insertando registros", point)
		if point == "....":
			point = ""
		try:
			cursor_origin.execute(query_insert_tel)
			db_origin.commit()
			flag = True
		except:
			db_origin.rollback()
			print("Error al insertar el registro en la cuenta ", tel_iidcuenta)
			flag = False
	if flag:
		print("_____________________________________ Registros insertados con exito _____________________________________")
	else:
		print("_____________________________________ OCURRIO UN ERROR AL INSERTAR LOS REGISTROS _____________________________________")
	
def cargaTelefonosBykom(loaderFile):
	patronAlpha = re.compile("[A-Z]+")
	os.system("clear")
	i = 1
	flag = False
	print("Insertando registros...")
	for line in loaderFile.readlines():
		posCadena = line.split(",")
		order_id = i
		order_rl = int(posCadena[0].strip("("))
		orden = 1
		tipotelefono = 1
		areacode = 100000000
		telefono = posCadena[4].strip()
		if patronAlpha.match(telefono):
			telefono = posCadena[5]
		predigito = 0
		posdigito = 0
		query_insert_tel = "INSERT INTO tlrlpersonas(ORDER_ID, ORDER_RL, ORDEN, TIPOTELEFONO, AREACODE, TELEFONO, PREDIGITO, POSTDIGITO) VALUES (%d,%d,%d,%d,%d,'%s',%d,%d)" %(order_id, order_rl, orden, tipotelefono, areacode, telefono, predigito, posdigito)
		
		try:
			cursor_destiny.execute(query_insert_tel)
			db_destiny.commit()
			flag = True
		except:
			db_destiny.rollback()
			print("Error al insertar el registro en la cuenta ", order_rl)
			flag = False
		i+=1
	if flag:
		print("_____________________________________ Registros insertados con exito _____________________________________")
	else:
		print("_____________________________________ OCURRIO UN ERROR AL INSERTAR LOS REGISTROS _____________________________________")

#def cargaUsuariosSoftguard(loaderFile):
	# A desarrollar
def cargaUsuariosBykom(loaderFile): #Esto insertara los datos en la tabla abrlusuarios en Bykom
	os.system("clear")
	i = 1
	for line in loaderFile.readlines():
		posCadena = line.split(",")
		n_cuenta = int(posCadena[0].strip("("))
		order_rl = obtenerID_CL(n_cuenta)
		i += 1
		#codigo_id  = Tipo de dato desconocido 
		cod_us  = int(posCadena[3])
		print("Posicion: ",i, " - ",order_rl, " - ",cod_us)
		#user_id  = 
	# clave  = 
	# contraclave  = 
	# datos01  = 
	# tipouser  = 
	# fechahora = 

def cargaZonasBykom(fileZonas):
	os.system("clear")
	for line in fileZonas.readlines():
		cadena = line.split(",")
		

def imprimeMenu(): 
	
	print("______________________________________________________________________________________________")
	print()
	print("------------------------------------ Migración a Bykom ---------------------------------------")
	print("------------------------------- por Agustin Ducca Pantaleon ----------------------------------")
	print("______________________________________________________________________________________________")
	print()
	print("Seleccione una opción: ")
	print("1.- Formatear datos de cuentas de Softguard y cargar en Bykom por numero de Orden")
	print("2.- Obtener Numero de Orden en cuenta de Bykom")
	print("3.- Obtener Usuarios de cuenta en Bykom")
	print("4.- Obtener Usuarios de cuenta en Softguard")
	print("5.- Carga masiva en Tabla Personas de Bykom")
	print("6.- Carga masiva de Usuarios en Bykom") # Falta informacion en tabla
	print("7.- Carga masiva de Telefonos en Softguard")
	print("8.- Carga masiva de Telfonos en Bykom")
	print("9.- Carga masiva de Usuarios en Softguard")
	print("10.- Realizar query")
	print("99.- Salir")

def limpiarPantallaTitular(mensaje):
	os.system('clear')
	print(mensaje)
	print()

#Metodo principal
	
if __name__ == '__main__':

	#Definición de variables globales

	db_origin = conexionDefault("softguard")
	db_destiny = conexionDefault("bykom")
	cursor_origin = cursorMaker(db_origin)
	cursor_origin = db_origin.cursor(buffered=True)
	cursor_destiny = cursorMaker(db_destiny)
	cursor_destiny = db_destiny.cursor(buffered=True)
	opcion = 0
	cuentaIngresada = 0

	# Menu

	while opcion != 99:
		imprimeMenu()
		print()
		opcion = int(input("Opción: "))

		if opcion == 1:
			limpiarPantallaTitular("____________________ Formatear datos de cuentas de Softguard y cargar en Bykom ______________")
			nombre,apellido,ids = formatearDatosUsuariosSoftguard(cursor_origin, cursor_destiny)
			actualizacionCodigosBykom(nombre,apellido,ids)
			print("_____________________________________ Proceso finalizado _____________________________________")
			print()
		elif opcion == 2:
			limpiarPantallaTitular("________________________ Obtener Numero de Orden en cuenta de Bykom ___________________________")
			cuentaIngresada = int(input("Ingrese número de cuenta: "))
			print("Número de Orden: ", obtenerID_CL(cuentaIngresada))
			print()
		elif opcion == 3:
			limpiarPantallaTitular("___________________________ Obtener usuarios de cuentas en Bykom ______________________________")
			cuentaIngresada = int(input("Ingrese número de cuenta: "))
			print(obtenerUsuariosporCuenta(cuentaIngresada))
		elif opcion == 4:
			limpiarPantallaTitular("___________________________ Obtener Usuarios de cuenta en Softguard ___________________________")
			cuentaIngresada = int(input("Ingrese número de cuenta: "))
			print(obtenerUsuariosSoftguard(cuentaIngresada))
		elif opcion == 5:
			limpiarPantallaTitular("_____________________________ Carga masiva en Tabla Personas de Bykom _____________________________")
			nombre,apellido,ids = formatearDatosUsuariosSoftguard(cursor_origin, cursor_destiny)
			cargaMasivaPersonasBykom(nombre,apellido,ids)
		elif opcion == 6:
			limpiarPantallaTitular("________________________________ Carga masiva de Usuarios en Bykom ________________________	________")
			loaderFileU = open("m_usuarios.sql", "r", encoding='latin-1')
			cargaUsuariosBykom(loaderFileU)
		elif opcion == 7:
			limpiarPantallaTitular("________________________________ Carga masiva de Telefonos en Softguard ________________________________")
			loaderFile = open("m_telefonos.sql", "r", encoding='UTF8')
			cargaTelefonosSoftguard(loaderFile)
		elif opcion == 8:
			limpiarPantallaTitular("_____________________________________ Carga masiva de Telefonos en Bykom _____________________________________")
			loaderFileB = open("m_telefonos.sql", "r", encoding='UTF8')
			cargaTelefonosBykom(loaderFileB)
		elif opcion == 9:
			limpiarPantallaTitular("_____________________________________ Carga masiva de Usuarios en Softguard _____________________________________")
			loaderFileU = open("m_usuarios.sql", "r", encoding='latin-1')
			cargaUsuariosSoftguard(loaderFileU)
