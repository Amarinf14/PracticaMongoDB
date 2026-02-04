package com.accesodatos.ad06_alberto_marin;

import com.accesodatos.ad06_alberto_marin.conexion.Conexion;
import com.accesodatos.ad06_alberto_marin.consulta.Consultas;
import com.accesodatos.ad06_alberto_marin.operacion.Operacion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

/**
 * Clase principal del proyecto
 * 
 * @author Alberto Marín Fernández
 */
public class Main {
    public static void main(String[] args) {
        // 1. Se conecta a MongoDB Atlas mediante la cadena de conexión configurada en nuestra clase
        Conexion conexion = new Conexion("tarea06_Alberto_Marin", "tarea06", "tarea06");
        
        // 2. Obtener el cliente y la base de datos
        try (MongoClient cliente = conexion.getCliente()) {
            if (cliente == null) return;

            MongoDatabase db = conexion.getDatabase(cliente);
            
            // 3. Obtener las colecciones definidas en el archivo JSON
            MongoCollection<Document> coleccion = conexion.getColeccion(db, "coffe_shop");
            
            // 4. Instanciar la lógica de las operaciones
            Operacion operacion = new Operacion(coleccion);
            System.out.println("\n=== RESULTADOS DE LAS OPERACIONES MONGO ===\n");
            
            operacion.insertarCliente();
            operacion.modificarCliente();
            operacion.borrarCliente();

            // 5. Instanciar la lógica de las consultas
            Consultas consultas = new Consultas(coleccion);
            System.out.println("=== RESULTADOS DE LAS CONSULTAS MONGO ===\n");

            // a) Media de edad de los clientes
            System.out.printf("a) Media de edad: %.2f años%n", consultas.mediaEdad());

            // b) Clientes con membresía >= 4 y edad > 35
            System.out.println("b) Clientes Nivel >= 4 y Edad > 35:");
            consultas.clientesNivelEdad().forEach(doc -> 
                System.out.println("   - " + doc.getString("Name")));

            // c) Nombre e ID con gasto > 5.1€
            System.out.println("c) Clientes con gasto total > 5.1€:");
            consultas.clientesGastoMayor51().forEach(doc -> 
                System.out.println("   - ID: " + doc.get("Member_ID") + " | Name: " + doc.get("Name") + " | Total: " + doc.get("total")));

            // d) Media de dinero en happy_hour
            System.out.printf("d) Media gasto Happy Hour: %.2f€%n", consultas.mediaGastoHappyHour());

            // e) Clientes > 15 min compra, orden alfabético
            System.out.println("e) Clientes con compra > 15 min (Ordenados):");
            consultas.clientesMas15Min().forEach(doc -> 
                System.out.println("   - " + doc.getString("Name")));

            // f) Todos los datos de clientes tarjeta "Black"
            System.out.println("f) Listado de miembros con tarjeta Black:");
            consultas.clientesTarjetaBlack().forEach(doc -> 
                System.out.println("   - " + doc.toJson()));

        } catch (Exception e) {
            System.err.println("Error en la ejecución: " + e.getMessage());
        }
    }
}
