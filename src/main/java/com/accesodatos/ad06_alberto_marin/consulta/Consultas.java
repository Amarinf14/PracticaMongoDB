package com.accesodatos.ad06_alberto_marin.consulta;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.*;
import java.util.Arrays;
import org.bson.Document;

/**
 *
 * @author macra
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
                Aggregates.unwind("$member"),
                Aggregates.group(null, Accumulators.avg("media", "$member.Age"))
        )).first();
        return resultado != null && resultado.get("media") != null ? ((Number) resultado.get("media")).doubleValue(): 0.0;
    }

    // b) Clientes con nivel de membresía >= 4 y edad > 35
    public AggregateIterable<Document> clientesNivelEdad() {
        // Uso de Filters para combinar condiciones GTE (>=) y GT (>)
        return collection.aggregate(Arrays.asList(
                Aggregates.unwind("$member"),
                Aggregates.match(Filters.and(
                        Filters.gte("member.Level_of_membership", 4),
                        Filters.gt("member.Age", 35)
                )),
                Aggregates.replaceRoot("$member") // Devolvemos el cliente directamente
        ));
    }

    // c) Nombre e ID de clientes que gastaron > 5.1€ en happy_hour + cantidad
    public AggregateIterable<Document> clientesGastoMayor51() {
        // Dado que Total_amount es String en el JSON, se convierte a double
        return collection.aggregate(Arrays.asList(
                Aggregates.unwind("$happy_hour_member"),
                Aggregates.unwind("$member"),
                
                // Filtramos que el ID del gasto coincida con el ID del miembro
                Aggregates.match(Filters.expr(
                        new Document("$eq", Arrays.asList("$happy_hour_member.Member_ID", "$member.Member_ID"))
                )),
                
                // Proyectamos los campos limpios para el Main
                Aggregates.project(Projections.fields(
                        Projections.computed("Member_ID", "$happy_hour_member.Member_ID"),
                        Projections.computed("Name", "$member.Name"),
                        Projections.computed("total", new Document("$toDouble", "$happy_hour_member.Total_amount"))
                )),
                
                // Filtramos la cantidad mínima de gasto
                Aggregates.match(Filters.gt("total", 5.1))
        ));
    }

    // d) Media de dinero gastado en happy_hour
    public double mediaGastoHappyHour() {
        // Conversión de Total_amount (String) a Double para calcular el promedio
        Document resultado = collection.aggregate(Arrays.asList(
                Aggregates.unwind("$happy_hour_member"),
                Aggregates.group(null, Accumulators.avg("mediaGasto", 
                        new Document("$toDouble", "$happy_hour_member.Total_amount")))
        )).first();
        return resultado != null && resultado.get("mediaGasto") != null ? resultado.getDouble("mediaGasto") : 0.0;
    }

    // e) Nombres clientes >15 min compra, orden alfabético
    public AggregateIterable<Document> clientesMas15Min() {
        // Filtro por Time_of_purchase, ordenación por Name y proyección del nombre
        return collection.aggregate(Arrays.asList(
                Aggregates.unwind("$member"),
                Aggregates.match(Filters.gt("member.Time_of_purchase", 15)),
                Aggregates.sort(Sorts.descending("member.Time_of_purchase")),
                Aggregates.project(Projections.computed("Name", "$member.Name"))
        ));
    }

    // f) Todos los datos clientes tarjeta "Black"
    public AggregateIterable<Document> clientesTarjetaBlack() {
        return collection.aggregate(Arrays.asList(
                Aggregates.unwind("$member"),
                Aggregates.match(Filters.eq("member.Membership_card", "Black")),
                Aggregates.replaceRoot("$member")
        ));
    }
}
