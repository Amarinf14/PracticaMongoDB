package com.accesodatos.ad06_alberto_marin.consulta;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.FindIterable;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.*;
import java.util.Arrays;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 *
 * @author dam207
 */
public class Consultas {

    private MongoCollection<Document> collection;

    public Consultas(MongoCollection<Document> collection) {
        this.collection = collection;
    }

    // a) Media de la edad de los clientes
    public double mediaEdad() {
        // Se utiliza el framework de Aggregation con el acumulador avg
        Document resultado = collection.aggregate(Arrays.asList(
                Aggregates.group(null, Accumulators.avg("media", "$Age"))
        )).first();
        return resultado != null && resultado.get("media") != null ? ((Number) resultado.get("media")).doubleValue(): 0.0;
    }

    // b) Clientes con nivel de membresía >= 4 y edad > 35
    public FindIterable<Document> clientesNivelEdad() {
        // Uso de Filters para combinar condiciones GTE (>=) y GT (>)
        Bson filtro = Filters.and(
                Filters.gte("Level_of_membership", 4),
                Filters.gt("Age", 35)
        );
        return collection.find(filtro);
    }

    // c) Nombre e ID de clientes que gastaron > 5.1€ en happy_hour + cantidad
    public AggregateIterable<Document> clientesGastoMayor51() {
        // Dado que Total_amount es String en el JSON, se convierte a double
        return collection.aggregate(Arrays.asList(
                Aggregates.project(Projections.fields(
                        Projections.include("Name", "Member_ID"),
                        // Cálculo: se suma el valor convertido de Total_amount y el campo cantidad
                        new Document("total", new Document("$add", 
                                Arrays.asList(new Document("$toDouble", "$Total_amount"), "$cantidad")))
                )),
                Aggregates.match(Filters.gt("total", 5.1))
        ));
    }

    // d) Media de dinero gastado en happy_hour
    public double mediaGastoHappyHour() {
        // Conversión de Total_amount (String) a Double para calcular el promedio
        Document resultado = collection.aggregate(Arrays.asList(
                Aggregates.group(null, Accumulators.avg("mediaGasto", 
                        new Document("$toDouble", "$Total_amount")))
        )).first();
        return resultado != null && resultado.get("mediaGasto") != null ? resultado.getDouble("mediaGasto") : 0.0;
    }

    // e) Nombres clientes >15 min compra, orden alfabético
    public AggregateIterable<Document> clientesMas15Min() {
        // Filtro por Time_of_purchase, ordenación por Name y proyección del nombre
        return collection.aggregate(Arrays.asList(
                Aggregates.match(Filters.gt("Time_of_purchase", 15)),
                Aggregates.sort(Sorts.ascending("Name")),
                Aggregates.project(Projections.include("Name"))
        ));
    }

    // f) Todos los datos clientes tarjeta "Black"
    public FindIterable<Document> clientesTarjetaBlack() {
        // Operación CRUD simple utilizando el filtro de igualdad
        return collection.find(Filters.eq("Membership_card", "Black"));
    }
}
