package com.accesodatos.ad06_alberto_marin.conexion;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 *
 * @author dam207
 */
public class Conexion {

    private static String dbNombre = "tarea06_Alberto_Marin";
    private static String usuario = "tarea06";
    private static String password = "tarea06";

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    /**
     * Constructor de la clase Conexion.
     *
     * @param dbNombre Nombre de la base de datos a la que conectarse
     * @param usuario Nombre de usuario para la autenticación
     * @param password Contraseña para la autenticación
     */
    public Conexion(String dbNombre, String usuario, String password) {
        Conexion.dbNombre = dbNombre;
        Conexion.usuario = usuario;
        Conexion.password = password;
    }

    /**
     * Establece y devuelve una conexión con el servidor MongoDB. Utiliza la
     * cadena de conexión específica para MongoDB Atlas.
     *
     * @return MongoClient configurado para conectarse al cluster de MongoDB
     * Atlas, o null si ocurre un error durante la conexión
     * @see MongoClient
     */
    public MongoClient getCliente() {

        try {
            String connectionString = "mongodb+srv://" + usuario + ":" + password + "@cluster0.jqbnyfe.mongodb.net/?appName=Cluster0";

            // Configurar Server API para usar la versión estable de MongoDB
            ServerApi serverApi = ServerApi.builder()
                    .version(ServerApiVersion.V1)
                    .build();

            // Crear configuración del cliente con la cadena de conexión
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(new com.mongodb.ConnectionString(connectionString))
                    .serverApi(serverApi)
                    .build();

            // Crear y devolver el cliente MongoDB
            return MongoClients.create(settings);

        } catch (Exception e) {
            // Manejar error de conexión
            System.err.println("Error al crear cliente MongoDB Atlas: " + e.getMessage());
            return null;
        }
    }

    /**
     * Obtiene la base de datos dada en el constructor a partir de un cliente
     * MongoDB.
     *
     * @param cliente Cliente MongoDB previamente conectado
     * @return MongoDatabase correspondiente al nombre de base de datos
     * proporcionado, o null si ocurre un error
     * @see MongoDatabase
     */
    public MongoDatabase getDatabase(MongoClient cliente) {
        try {
            // Obtener la base de datos usando el nombre proporcionado en el constructor
            return cliente.getDatabase(dbNombre);
        } catch (Exception e) {
            // Manejar error al acceder a la base de datos
            System.err.println("Error al obtener base de datos: " + e.getMessage());
            return null;
        }
    }

    /**
     * Obtiene una colección de una base de datos MongoDB.
     *
     * @param baseDatos Base de datos de la cual obtener la colección
     * @param nombreColeccion Nombre de la colección a buscar
     * @return MongoCollection<Document> correspondiente a la colección
     * solicitada, o null si ocurre un error
     * @see MongoCollection
     * @see Document
     */
    public MongoCollection<Document> getColeccion(MongoDatabase baseDatos, String nombreColeccion) {
        try {
            // Obtener la colección dentro de la base de datos
            return baseDatos.getCollection(nombreColeccion);
        } catch (Exception e) {
            // Manejar error al acceder a la colección
            System.err.println("Error al obtener colección: " + e.getMessage());
            return null;
        }
    }
}
