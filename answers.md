# ANSWERS — InDrive Analytics avec dbt


## Q1.  Pourquoi 8 et pas 200 ? Combien de partitions par cœur CPU viserais-tu, et qu'est-ce qui ne va pas avec trop peu ou trop de partitions ?

La règle de base est de viser 2 à 4 partitions par cœur CPU, soit 8 à 16 partitions. Mettre 200 partitions sur 4 cœurs, c'est payer 192 tâches de surcoût (création, scheduling, sérialisation) pour rien.

## Q2.  Quelle ligne a réellement lancé le job Spark, et pourquoi la construction de plan a-t-elle pris presque aucun temps ?

Quelle ligne a lancé le job ? plan.count() (dans la cellule d'assert). C'est une action, seul type d'opération qui déclenche l'exécution.

Pourquoi la construction a pris ~0 s ? Spark est lazy : filter, withColumn, groupBy, agg ne sont que des déclarations. Catalyst optimise tout le plan d'un coup au moment de l'action (fusion de projections, pushdown, etc.), ce qui serait impossible si chaque transformation s'exécutait isolément.

## Q3.  Pourquoi la version broadcast évite-t-elle de shuffler la grosse table, et qu'est-ce qui limite la taille de la table diffusée ? Pourquoi a-t-il fallu un hint pour obtenir la version lente ?

Pourquoi le broadcast évite-t-il le shuffle ? Parce que la petite table est copiée sur chaque executor. Chaque tâche traite sa partition locale de la grosse table et fait la jointure contre une hashmap en RAM. Zéro échange réseau pour la grosse table.

Ce qui limite la taille diffusée : spark.sql.autoBroadcastJoinThreshold (par défaut ~10 Mo). Au-delà, la table ne tient plus raisonnablement en mémoire sur chaque executor et le coût de diffusion devient prohibitif.

Pourquoi le hint est-il nécessaire ? Parce que Spark, par défaut, aurait déjà choisi le broadcast (8 lignes = bien en dessous du seuil). Le hint "merge" sert purement à forcer pédagogiquement la version lente pour comparer.

## Q4.  Où verrais-tu ce skew dans le Spark UI (ou le query profile en serverless), et que chercherais-tu exactement ?

Spark UI classique (Docker) : onglet Jobs → Stage → tab "Tasks". On cherche une task dont Duration et Shuffle Read Size sont beaucoup plus élevés que la médiane (ex. 48 Mo vs 4 Mo, cf. slide 17).

Query profile (serverless Databricks) : dans le graphe d'opérateurs, on regarde les métriques par task sous un nœud Exchange ou HashAggregate — un écart max/médiane élevé = skew.

Signaux à chercher : Task Duration distribution très asymétrique, Shuffle Read Size max ≫ médiane, Spill (disk) concentré sur une task.


## Q5.  running est utilisé plusieurs fois dans ton analyse. cache() aiderait-il, et pourquoi pourrait-il ne pas être disponible sur Databricks Free Edition ? Quelle est l'alternative ?

cache() aiderait-il ? Oui : running est utilisé plusieurs fois (.count(), analyse, export). Le cache éviterait de recalculer à chaque action le groupBy + les fenêtres sur 2M lignes.

Pourquoi pas dispo ? Sur Databricks Free Edition serverless, cache() n'est pas supporté (le compute est éphémère et le cache persisté n'a pas de sens entre invocations).

Alternative : écrire running dans une table Delta (running.write.format("delta").saveAsTable("indrive_running")) puis lire la table à chaque usage. C'est de la persistance durable, et ça sert aussi de point de reprise pour la suite du pipeline.


## Q6.  Quelles étapes de ce notebook planifierais-tu comme Job Databricks, et lesquelles sont de la simple exploration ?

À planifier comme Job Databricks (pipeline réutilisable, paramétrable, à rafraîchir périodiquement) :

    Génération/ingestion des rides (ou lecture depuis une source réelle)

    Construction de daily → running → écriture Delta

    Le MERGE sur indrive_dim_driver (mise à jour des notes chauffeurs)

    Écriture finale de la table agrégée

Simple exploration (à faire en notebook ad-hoc) :

    partition_sizes() pour vérifier les partitions

    explain() / plan_text() pour lire Catalyst

    Comparaison de timings shuffle vs broadcast

    Tests de skew et hot_share

## 