package com.accesodatos.ad06_alberto_marin.operacion;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import java.util.Arrays;
import org.bson.Document;

/**
 *
 * @author macra
 */
public class Operacion {

    private MongoCollection<Document> collection;

    public Operacion(MongoCollection<Document> collection) {
        this.collection = collection;
    }

    // a) INSERTAR un nuevo cliente en la colección con mi nombre y apellido
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

        // Insertamos el cliente
        collection.updateOne(
                Filters.exists("member"),
                Updates.push("member", nuevoCliente)
        );

        // Mostramos todos los clientes
        System.out.println("Cliente insertado satisfactoriamente.\nListado de clientes:");
        mostrarClientes();
    }

    // b) MODIFICAR el cliente con mi nombre para que tenga un tiempo de compra de '99'
    public void modificarCliente() {
        collection.updateOne(
                Filters.eq("member.Name", "Marín, Alberto"),
                Updates.set("member.$[cliente].Time_of_purchase", 99),
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

    // c) BORRAR al cliente con mi nombre
    public void borrarCliente() {

        // Como filtro de búsqueda, tendremos 'Time_of_purchase = 99'
        collection.updateOne(
                Filters.exists("member"),
                Updates.pull(
                        "member",
                        new Document("Time_of_purchase", 99)
                )
        );
        
        // Mostramos todos los clientes
        System.out.println("Cliente borrado satisfactoriamente:");
        mostrarClientes();
    }

    /**
     * === MÉTODO AUXILIAR ===
     */
    // Mostrar clientes
    private void mostrarClientes() {
        Document doc = collection.find().first();
        if (doc != null) {
            doc.getList("member", Document.class)
                    .forEach(cliente
                            -> System.out.println(" - " + cliente.toJson()));
        }
        System.out.println("-".repeat(25));
    }
}