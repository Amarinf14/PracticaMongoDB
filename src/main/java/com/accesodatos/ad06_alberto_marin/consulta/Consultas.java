package com.accesodatos.ad06_alberto_marin.consulta;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.*;
import java.util.Arrays;
import org.bson.Document;

/**
 * Contiene distintos métodos de consulta utilizando el framework de
 * agregaciones de MongoDB Cada método implementa una operación específica
 * solicitada en la práctica
 *
 * @author Alberto Marín Fernández
 */
public class Consultas {

    // Referencia a la colección MongoDB
    private MongoCollection<Document> collection;

    /**
     * Constructor de la clase Recibe la colección sobre la que se ejecutarán
     * las consultas.
     *
     * @param collection referencia a la colección MongoDB
     */
    public Consultas(MongoCollection<Document> collection) {
        this.collection = collection;
    }

    /**
     * a) Media de edad de los clientes
     *
     * Se utiliza el framework de Aggregation con el acumulador avg
     *
     * @return la media de edad de los clientes 0 si no hay resultados
     */
    public double mediaEdad() {

        Document resultado = collection.aggregate(Arrays.asList(
                Aggregates.unwind("$member"), // Descomoponemos el array "member" en documentos individuales
                Aggregates.group(null, Accumulators.avg("media", "$member.Age")) // Agrupamos todos los documentos y calculamos la media
        )).first(); // Tomamos el único documento

        return resultado != null && resultado.get("media") != null ? ((Number) resultado.get("media")).doubleValue() : 0.0;
    }

    /**
     * b) Clientes con nivel de membresía >= 4 y edad > 35
     *
     * Uso de Filters para combinar condiciones GTE (>=) y GT (>)
     *
     * @return los clientes que cumplen el criterio
     */
    public AggregateIterable<Document> clientesNivelEdad() {

        return collection.aggregate(Arrays.asList(
                Aggregates.unwind("$member"), // Descomoponemos el array "member" en documentos individuales
                Aggregates.match(Filters.and(
                        Filters.gte("member.Level_of_membership", 4),
                        Filters.gt("member.Age", 35)
                )), // Filtramos por nivel de membresía y edad
                Aggregates.replaceRoot("$member") // Devolvemos el documento del cliente
        ));
    }

    /**
     * c) Nombre e ID de clientes que gastaron > 5.1€ en happy_hour + cantidad
     *
     * Es necesario relacionar happy_hour_member con member mediante Member_ID y
     * convertir Total_amount (String) a Double
     *
     * @return documentos con Member_ID, Name y Total (Double) de clientes
     * filtrados
     */
    public AggregateIterable<Document> clientesGastoMayor51() {

        return collection.aggregate(Arrays.asList(
                Aggregates.unwind("$happy_hour_member"), // Descomoponemos el array "happy_hour_member" en documentos individuales
                Aggregates.unwind("$member"), // Descomoponemos el array "member" en documentos individuales

                Aggregates.match(Filters.expr(
                        new Document("$eq", Arrays.asList("$happy_hour_member.Member_ID", "$member.Member_ID"))
                )), // Relacionamos ambas estructuras usando 'Member_ID'

                Aggregates.project(Projections.fields(
                        Projections.computed("Member_ID", "$happy_hour_member.Member_ID"),
                        Projections.computed("Name", "$member.Name"),
                        Projections.computed("total", new Document("$toDouble", "$happy_hour_member.Total_amount")) // Conversión String a Double
                )), // Proyectamos únicamente los campos necesarios

                Aggregates.match(Filters.gt("total", 5.1)) // Filtramos la cantidad mínima de gasto
        ));
    }

    /**
     * d) Media de dinero gastado en happy_hour
     *
     * Se convierte el campo Total_amount a Double antes de calcular el promedio
     *
     * @return la media de dinero gastado 0 si no hay resultados
     */
    public double mediaGastoHappyHour() {

        Document resultado = collection.aggregate(Arrays.asList(
                Aggregates.unwind("$happy_hour_member"), // Descomoponemos el array "happy_hour_member" en documentos individuales

                Aggregates.group(null, Accumulators.avg("mediaGasto",
                        new Document("$toDouble", "$happy_hour_member.Total_amount"))) // Calculamos la media del gasto
        )).first(); // Tomamos el único documento

        return resultado != null && resultado.get("mediaGasto") != null ? resultado.getDouble("mediaGasto") : 0.0;
    }

    /**
     * e) Nombres clientes >15 min compra, orden alfabético
     *
     * Filtrado por Time_of_purchase > 15, ordenación descendente por nombre y
     * proyección del nombre del cliente
     *
     * @return los nombres de clientes ordenados alfabéticamente
     */
    public AggregateIterable<Document> clientesMas15Min() {
        
        return collection.aggregate(Arrays.asList(
                Aggregates.unwind("$member"),// Descomoponemos el array "member" en documentos individuales
                Aggregates.match(Filters.gt("member.Time_of_purchase", 15)), // Filtramos compras > 15 minutos
                Aggregates.sort(Sorts.descending("member.Time_of_purchase")), // Ordenamos por tiempo descendente
                Aggregates.project(Projections.computed("Name", "$member.Name")) // Proyectamos solo el nombre
        ));
    }

    /**
     * f) Todos los datos clientes tarjeta "Black"
     *
     * Filtrado por Membership_card = "Black" con replaceRoot para devolver el
     * documento completo del cliente
     *
     * @return todos los clientes con tarjeta Black
     */
    public AggregateIterable<Document> clientesTarjetaBlack() {
        return collection.aggregate(Arrays.asList(
            Aggregates.unwind("$member"), // Descomoponemos el array "member" en documentos individuales
            Aggregates.match(Filters.eq("member.Membership_card", "Black")), // Filtramos por tarjeta Black
            Aggregates.replaceRoot("$member") // Devolvemos el documento del cliente
        ));
    }
}
