package com.accesodatos.ad06_alberto_marin;

import com.accesodatos.ad06_alberto_marin.conexion.Conexion;
import com.accesodatos.ad06_alberto_marin.consulta.Consultas;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

/**
 *
 * @author dam207
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

            // 4. Instanciar la lógica de consultas
            Consultas consultas = new Consultas(coleccion);
            System.out.println("=== RESULTADOS DE LAS OPERACIONES MONGO ===\n");

            // a) Media de edad de los clientes
            // Utiliza el campo "Age" del JSON [4].
            System.out.printf("a) Media de edad: %.2f años%n", consultas.mediaEdad());

            // b) Clientes con membresía >= 4 y edad > 35
            // Filtra por "Level_of_membership" y "Age" [4, 6].
            System.out.println("b) Clientes Nivel >= 4 y Edad > 35:");
            consultas.clientesNivelEdad().forEach(doc -> 
                System.out.println("   - " + doc.getString("Name")));

            // c) Nombre e ID con gasto > 5.1€
            // En el JSON, "Total_amount" es un String, por lo que se requiere conversión [5, 7].
            System.out.println("c) Clientes con gasto total > 5.1€:");
            consultas.clientesGastoMayor51().forEach(doc -> 
                System.out.println("   - ID: " + doc.get("Member_ID") + " | Name: " + doc.get("Name") + " | Total: " + doc.get("total")));

            // d) Media de dinero en happy_hour
            // Procesa el promedio del campo "Total_amount" [7].
            System.out.printf("d) Media gasto Happy Hour: %.2f€%n", consultas.mediaGastoHappyHour());

            // e) Clientes > 15 min compra, orden alfabético
            // Filtra por "Time_of_purchase" y ordena por "Name" [4, 8].
            System.out.println("e) Clientes con compra > 15 min (Ordenados):");
            consultas.clientesMas15Min().forEach(doc -> 
                System.out.println("   - " + doc.getString("Name")));

            // f) Todos los datos de clientes tarjeta "Black"
            // Busca coincidencia exacta con "Membership_card": "Black" [4, 6].
            System.out.println("f) Listado de miembros con tarjeta Black:");
            consultas.clientesTarjetaBlack().forEach(doc -> 
                System.out.println("   - " + doc.toJson()));

        } catch (Exception e) {
            System.err.println("Error en la ejecución: " + e.getMessage());
        }
    }
}
