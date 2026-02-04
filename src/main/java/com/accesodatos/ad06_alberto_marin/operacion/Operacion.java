package com.accesodatos.ad06_alberto_marin.operacion;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import java.util.Arrays;
import org.bson.Document;

/**
 * Contiene métodos para realizar operaciones CRUD sobre una colección MongoDB
 * Se trabaja directamente sobre los documentos mediante la API oficial de MongoDB para Java.
 * 
 * @author Alberto Marín Fernández
 */
public class Operacion {

    // Referencia a la colección MongoDB
    private MongoCollection<Document> collection;

    /**
     * Constructor de la clase
     * Recibe la colección sobre la que se realizarán las operaciones
     */
    public Operacion(MongoCollection<Document> collection) {
        this.collection = collection;
    }

    /**
     * a) INSERTAR un nuevo cliente en la colección
     *
     * Creamos un documento con los datos del cliente y se añade al array "member" usando el operador $push
     */
    public void insertarCliente() {

        // Creamos el nuevo cliente con una estructura idéntica al JSON
        Document nuevoCliente = new Document()
                .append("Member_ID", 45)
                .append("Name", "Marín, Alberto")
                .append("Membership_card", "Black")
                .append("Age", 30)
                .append("Time_of_purchase", 40)
                .append("Level_of_membership", 1)
                .append("Address", "Santoña");

        // Insertamos el cliente dentro del array 'member'
        collection.updateOne(
                Filters.exists("member"), // Localizamos el documento que contiene al array
                Updates.push("member", nuevoCliente) // Añadimos eñ nuevo cliente
        );

        // Mostramos todos los clientes
        System.out.println("Cliente insertado satisfactoriamente.\nListado de clientes:");
        mostrarClientes();
    }

    /**
     * b) MODIFICAR el cliente con nombre "Marín, Alberto"
     *
     * Actualizamos el campo Time_of_purchase a 99 utilizando filtros sobre arrays
     */
    public void modificarCliente() {
        collection.updateOne(
                Filters.eq("member.Name", "Marín, Alberto"), // Localizamos el documento que contiene al cliente mediante su nombre
                Updates.set("member.$[cliente].Time_of_purchase", 99), // Modificamos el campo del cliente encontrado
                
                // ArrayFilters permite aplicar el cambio solo al elemento del array que cumpla la condición
                new UpdateOptions().arrayFilters(
                        Arrays.asList(
                                Filters.eq("cliente.Name", "Marín, Alberto")
                        )
                )
        );

        // Mostramos todos los clientes
        System.out.println("Cliente modificado satisfactoriamente:");
        mostrarClientes();
    }

    /**
     * c) BORRAR el cliente previamente modificado
     *
     * Eliminamos del array "member" el documento cuyo Time_of_purchase sea 99
     */
    public void borrarCliente() {
        
        collection.updateOne(
                Filters.exists("member"), // Localizamos el documento que contiene al array
                Updates.pull(
                        "member",
                        new Document("Time_of_purchase", 99)
                ) // Eliminamos del array el objeto que cumpla el criterioo
        );
        
        // Mostramos todos los clientes
        System.out.println("Cliente borrado satisfactoriamente:");
        mostrarClientes();
    }

    /**
     * MÉTODO AUXILIAR
     *
     * Recupera el primer documento de la colección y muestra por consola todos los clientes almacenados dentro del array "member"
     */
    private void mostrarClientes() {
        
        // Obtenemos el primer documento de la colección
        Document doc = collection.find().first();
        if (doc != null) {
            // Recorremos la lista "member" y mostramos cada cliente en formato JSON
            doc.getList("member", Document.class)
                    .forEach(cliente
                            -> System.out.println(" - " + cliente.toJson()));
        }
        System.out.println("-".repeat(25));
    }
}