# Sistema para migracion de sistema de SoftGuard a Bykom Realizado por Agustin Ducca Pantaleon

import mysql.connector
import os

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
	lista_result = list(result)
	str_result = str(lista_result[0])
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

def cargaTelefonos():
	loaderFile = open("m_telefonos.sql", "r", encoding='UTF8')
	for line in loaderFile.readlines():
		posCadena = line.split(",")
		tel_iidcuenta = posCadena[0].strip("(")
		tel_iid = posCadena[1]
		print("Tel ID: ",type(tel_iid), " Cadena: ",tel_iid)
		tel_iid = tel_iid.strip()
		tel_iid = int(tel_iid)
		print("Tel ID: ",type(tel_iid), " Cadena: ",tel_iid)
		# tel_iid = int(float(tel_iid))
		print("Pos convert: ", type(tel_iid))
		tel_cnombre = posCadena[2]
		tel_cobservacion = posCadena[3]
		tel_ctelefono = posCadena[4]
		query_insert_tel = "INSERT INTO m_telefonos(tel_iidcuenta, tel_iid, tel_cnombre, tel_cobservacion, tel_ctelefono) VALUES(%s,%s,%s,%s,%s)" %(tel_iidcuenta, tel_iid, tel_cnombre, tel_cobservacion, tel_ctelefono)
		try:
			cursor_origen.execute(query_insert_tel)
			db_origin.commit()
		except:
			db_origin.rollback()
			print("Error al insertar el registro en la cuenta ", tel_iidcuenta)
		# print("Cuenta: ",tel_iidcuenta ,"| Tel ID: ",tel_iid,"| Nombre: ",tel_cnombre , "| Observacion: ", tel_cobservacion, "| Telefono: ", tel_ctelefono)
	
def imprimeMenu(): 
	
	print("______________________________________________________________________________________________")
	print()
	print("------------------------------------ Migración a Bykom ---------------------------------------")
	print("______________________________________________________________________________________________")
	print()
	print("Seleccione una opción: ")
	print("1.- Formatear datos de cuentas de Softguard y cargar en Bykom por numero de Orden en cuenta de Bykom")
	print("3.- Obtener Usuarios de cuenta en Bykom")
	print("4.- Obtener Usuarios de cuenta en Softguard")
	print("5.- Carga masiva en Tabla Personas de Bykom")
	print("6.- Carga masiva de Usuarios en Bykom") # Falta informacion en tabla
	print("7.- Carga masiva de Telefonos en Bykom")
	print("8.- Carga masiva de zonas en Bykom")
	print("9.- Realizar query")
	print("99.- Salir")

def limpiarPantallaTitular(mensaje):
	os.system('clear')
	print(mensaje)
	print()


	
if __name__ == '__main__':

	#Definición de variables globales

	db_origin = conexionDefault("softguard")
	db_destiny = conexionDefault("bykom")
	cursor_origin = cursorMaker(db_origin)
	cursor_destiny = cursorMaker(db_destiny)
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
			# Falta informacion en tabla
		elif opcion == 7:
			limpiarPantallaTitular("________________________________ Carga masiva de Telefonos en Bykom ________________________________")
			cargaTelefonos()


