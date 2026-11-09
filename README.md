# Kafka Spring Cloud Stream

Application de démonstration basée sur **Spring Boot**, **Spring Cloud Stream**, **Apache Kafka** et **Kafka Streams**. Elle génère des événements de pages, les publie dans Kafka, applique un traitement par fenêtre de cinq secondes, puis expose le résultat dans un topic de sortie.

## Fonctionnalités

- Production automatique d'événements `PageEvent` toutes les 200 ms.
- Publication des événements dans le topic `t3`.
- Filtrage des événements dont `duration` est supérieur à `100`.
- Agrégation par page (`P1` ou `P2`) dans des fenêtres Kafka Streams de cinq secondes.
- Publication du nombre d'événements agrégés dans le topic `t4`.
- Consommation d'événements depuis le topic `t2`.
- Endpoint REST permettant de publier manuellement un événement.

## Architecture

```text
pageEventSupplier ──> t3 ──> KStreamFunction ──> t4
                                       │
                                       ├─ filtre : duration > 100
                                       ├─ groupByKey : nom de la page
                                       ├─ fenêtre : 5 secondes
                                       └─ sortie : compteur Long

t2 ──> pageEventConsumer
```

Le topic `t4` contient :

- **clé** : nom de la page (`P1` ou `P2`) ;
- **valeur** : nombre d'événements reçus dans la fenêtre correspondante (`Long`).

## Prérequis

- Java 21 ou supérieur ;
- Docker et Docker Compose ;
- Maven 3.9 ou le Maven Wrapper fourni (`./mvnw`).

## Démarrage de Kafka

Depuis la racine du projet :

```bash
docker compose up -d
```

Vérifier que les conteneurs sont démarrés :

```bash
docker compose ps
```

Le broker Kafka est accessible :

| Client | Adresse |
|---|---|
| Application exécutée sur la machine hôte | `localhost:9092` |
| Commandes exécutées dans le conteneur Kafka | `broker:29092` |

> Depuis le conteneur Kafka, utiliser `broker:29092`. Le port `9092` est le listener exposé pour les clients exécutés sur la machine hôte.

## Lancer l'application

Avec le Maven Wrapper :

```bash
./mvnw spring-boot:run
```

Ou avec Maven :

```bash
mvn spring-boot:run
```

L'application démarre par défaut sur `http://localhost:8080`.

## Publier un événement manuellement

L'endpoint `GET /publish` accepte les paramètres `name` et `topic` :

```bash
curl "http://localhost:8080/publish?name=P1&topic=t3"
```

Exemple de réponse :

```json
{
  "name": "P1",
  "user": "user1",
  "date": "2026-11-09T17:25:24.746+00:00",
  "duration": 7421
}
```

Pour alimenter le traitement Kafka Streams, le paramètre `topic` doit être `t3`. Le champ `duration` est généré automatiquement ; les événements dont la durée est inférieure ou égale à `100` sont ignorés par le flux.

## Lire les résultats du topic `t4`

Depuis le conteneur Kafka :

```bash
docker exec --interactive --tty sdia-kafka-broker \
  kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic t4 \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

Le paramètre `--from-beginning` permet de lire les messages déjà présents. Sans ce paramètre, Kafka commence à la position courante et attend uniquement les nouveaux messages.

Pour lister les topics :

```bash
docker exec sdia-kafka-broker \
  kafka-topics --bootstrap-server broker:29092 --list
```

Pour inspecter le topic `t4` :

```bash
docker exec sdia-kafka-broker \
  kafka-topics --bootstrap-server broker:29092 --describe --topic t4
```

## Configuration des bindings

Les bindings Spring Cloud Stream sont définis dans [`application.properties`](src/main/resources/application.properties) :

| Fonction | Direction | Topic |
|---|---|---|
| `pageEventConsumer` | entrée | `t2` |
| `pageEventSupplier` | sortie | `t3` |
| `KStreamFunction` | entrée | `t3` |
| `KStreamFunction` | sortie | `t4` |

Le supplier est interrogé toutes les 200 ms. Kafka Streams utilise une fenêtre de cinq secondes et conserve le résultat agrégé dans le store `count-store`.

## Tests

Exécuter les tests du projet :

```bash
./mvnw test
```

Construire le projet :

```bash
./mvnw clean package
```

## Arrêt de l'environnement

Arrêter les conteneurs :

```bash
docker compose down
```

Supprimer également les volumes associés :

```bash
docker compose down -v
```

## Dépannage

### Aucun message n'est affiché dans `t4`

Vérifier les points suivants :

1. L'application Spring Boot est bien démarrée.
2. Le consumer écoute `t4` avec `--bootstrap-server broker:29092` lorsqu'il est lancé dans le conteneur.
3. Le consumer est lancé avant de déclencher un nouvel événement, ou utilise `--from-beginning`.
4. L'événement arrive bien sur `t3`, et non sur `t2`.
5. Le topic `t4` contient des messages :

   ```bash
   docker exec sdia-kafka-broker \
     kafka-get-offsets --bootstrap-server broker:29092 --topic t4
   ```

Un résultat tel que `t4:0:0` signifie que le topic ne contient encore aucun message.

## Technologies

- Java 21
- Spring Boot 4
- Spring Cloud Stream
- Spring Cloud Stream Kafka Binder
- Spring Cloud Stream Kafka Streams Binder
- Apache Kafka 7.3
- Docker Compose

