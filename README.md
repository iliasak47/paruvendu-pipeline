<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
</head>
<body>

<h1>Projet Data Engineering – Pipeline Immobilier ParuVendu</h1>

<p>
Ce projet consiste à concevoir et implémenter un <strong>pipeline de données automatisé</strong> permettant la collecte, la transformation et l’analyse des annonces immobilières du site ParuVendu pour la région de Rouen. L’objectif est de mettre en œuvre une architecture Data Engineering complète intégrant l'orchestration, l'historisation exhaustive et la gouvernance des données.
</p>

<h2>Objectifs</h2>
<ul>
    <li>Automatisation de la collecte de données via scraping web et stockage brut sur AWS S3.</li>
    <li>Traitement distribué des données (Silver/Gold) avec PySpark.</li>
    <li>Mise en place d'une historisation temporelle pour analyser la persistance des annonces.</li>
    <li>Modélisation analytique et enrichissement métier avec dbt.</li>
    <li>Garantie de la qualité des données via des tests automatisés et une intégration continue (CI/CD).</li>
    <li>Orchestration complète du flux via Apache Airflow.</li>
</ul>

<h2>Architecture du pipeline</h2>

<pre>
1. Extraction (Raw) : Scraping HTML → AWS S3
2. Structuration (Silver) : Parsing HTML → CSV structuré (PySpark)
3. Raffinement (Gold) : Nettoyage, Regex et calculs de métriques (PySpark)
4. Historisation (History) : Consolidation Parquet pour analyse temporelle (PySpark)
5. Analytics (Athena) : Modélisation en couches (Staging/Marts) via dbt
6. Visualisation : Reporting décisionnel via Power BI
</pre>

<p>
Le flux est orchestré par un DAG Airflow s'exécutant dans un environnement conteneurisé incluant Java 17 pour le support de Spark.
</p>

<h2>Rôle de dbt et Gouvernance</h2>

<p>
dbt assure la <strong>couche de modélisation analytique</strong> et la gouvernance du projet :
</p>
<ul>
    <li><strong>Enrichissement (Seeds)</strong> : Utilisation de référentiels statiques pour mapper les typologies de logements (ex: Studio, T2) à partir du nombre de pièces.</li>
    <li><strong>Qualité des données</strong> : Tests d'intégrité via <code>dbt_expectations</code> pour valider les prix, les surfaces et les contraintes de non-nullité.</li>
    <li><strong>Observabilité (Freshness)</strong> : Contrôle automatisé de la fraîcheur des données sources sur S3 avec alertes en cas de retard de traitement.</li>
    <li><strong>CI/CD</strong> : Pipeline GitHub Actions automatisant la validation des modèles et l'exécution des tests à chaque modification.</li>
</ul>

<h2>Stack technique</h2>

<div class="stack">
    <span>Python 3.10 / PySpark 3.5.2</span>
    <span>Apache Airflow 3.1.0</span>
    <span>Docker / Docker Compose</span>
    <span>AWS S3 & Athena</span>
    <span>dbt-core / dbt-athena-community</span>
    <span>Java 17 (OpenJDK)</span>
    <span>Power BI</span>
</div>

<h2>Structure du projet</h2>

<pre>
paruvendu_pipeline/
├── .github/workflows/      # Intégration continue (dbt CI)
├── dags/
│   ├── scripts/            # Scripts PySpark (Scrape, Parse, Transform, Historize)
│   ├── dbt/
│   │   ├── models/         # Modèles Staging, Marts et Analytics
│   │   ├── seeds/          # Référentiels métiers (Typologies)
│   │   ├── analyses/       # Requêtes de persistance des annonces
│   │   └── dbt_project.yml # Configuration du projet dbt
│   └── paruvendu_pipeline.py # DAG d'orchestration Airflow
├── Dockerfile              # Image personnalisée (Airflow + Spark + dbt)
├── docker-compose.yaml     # Infrastructure locale
└── requirements.txt        # Dépendances Python
</pre>

<h2>Lancement du projet</h2>

<h3>Prérequis</h3>
<ul>
    <li>Docker et Docker Compose (minimum 4GB RAM alloués).</li>
    <li>Accès configuré à AWS (S3 et Athena).</li>
</ul>

<h3>Démarrage</h3>
<pre>
docker-compose up -d
</pre>
<p>
L'interface Airflow est accessible sur <code>http://localhost:8080</code>.
</p>

<h2>Exploitation analytique</h2>

<p>
Les modèles finaux permettent des analyses avancées :
</p>
<ul>
    <li><strong>Top Annonces</strong> : Identification des 20% des meilleures opportunités au m² via calcul de percentiles.</li>
    <li><strong>Évolution du marché</strong> : Suivi hebdomadaire des prix et volumes d'annonces.</li>
    <li><strong>Analyse de persistance</strong> : Mesure de la durée de visibilité des annonces pour identifier les biens à forte rotation.</li>
    <li><strong>Typologie</strong> : Segmentation du marché par type de logement (T2, T3, etc.).</li>
</ul>

<h2>Axes d’amélioration</h2>
<ul>
    <li>Mise en place d'un système de monitoring et d'alerting (Slack/Email) sur échec de tâche.</li>
    <li>Déploiement du pipeline sur un service managé (MWAA ou Kubernetes).</li>
    <li>Extension du scraping à d'autres zones géographiques.</li>
</ul>

<p>
Projet réalisé dans un cadre académique et professionnel, orienté vers les standards du Data Engineering moderne.
</p>

</body>
</html>