<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
</head>

<body>

<h1>Projet Data Engineering – Pipeline Immobilier ParuVendu</h1>

<p>
Ce projet consiste à concevoir et implémenter un <strong>pipeline de données automatisé</strong>
permettant la collecte, la transformation et l’analyse des annonces immobilières
du site ParuVendu pour la région de Rouen.
</p>

<p>
L’objectif principal est de mettre en œuvre une architecture Data Engineering complète,
inspirée des bonnes pratiques industrielles, intégrant orchestration, historisation
et exploitation analytique.
</p>

<h2>Objectifs</h2>
<ul>
  <li>Automatiser la collecte de données via scraping web</li>
  <li>Centraliser les données brutes dans AWS S3</li>
  <li>Mettre en place des couches de transformation (silver / gold)</li>
  <li>Historiser les données pour permettre l’analyse temporelle</li>
  <li>Modéliser les données analytiques avec dbt</li>
  <li>Orchestrer l’ensemble du pipeline avec Apache Airflow</li>
</ul>

<h2>Architecture du pipeline</h2>

<pre>
Scraping
  → AWS S3 (raw)
  → PySpark (silver)
  → PySpark (gold + historisation)
  → dbt (AWS Athena)
  → Analytics (Power BI)
</pre>

<p>
L’ensemble des traitements est orchestré par un DAG Airflow exécuté
dans un environnement Dockerisé.
</p>

<h2>Rôle de dbt dans le pipeline</h2>

<p>
dbt est utilisé comme une <strong>couche de modélisation analytique</strong>
au-dessus des données Gold produites par PySpark.
</p>

<p>
Les transformations lourdes (scraping, parsing HTML, nettoyage avancé,
normalisation et historisation) sont réalisées en amont avec PySpark.
dbt intervient ensuite via <strong>AWS Athena</strong> afin de structurer les données
selon des modèles analytiques, appliquer une logique métier légère
et produire des tables prêtes à l’exploitation dans Power BI.
</p>

<h2>Stack technique</h2>

<div class="stack">
  <span>Python</span>
  <span>Apache Airflow</span>
  <span>Docker / Docker Compose</span>
  <span>AWS S3</span>
  <span>AWS Athena</span>
  <span>dbt (dbt-athena)</span>
  <span>PySpark</span>
  <span>Power BI</span>
</div>

<h2>Structure du projet</h2>

<pre>
paruvendu_DE/
├── dags/              # DAG Airflow et scripts d’orchestration
├── config/            # Configuration Airflow
├── docker-compose.yaml
├── Dockerfile
└── requirements.txt
</pre>

<p>
Les données, logs, artefacts Spark et fichiers de configuration sensibles
ne sont pas versionnés.
</p>

<h2>Lancement du projet</h2>

<h3>Prérequis</h3>
<ul>
  <li>Docker et Docker Compose</li>
  <li>Accès AWS (S3 et Athena)</li>
</ul>

<h3>Démarrage</h3>

<pre>
docker-compose up -d
</pre>

<p>
L’interface Airflow est accessible à l’adresse suivante :
<code>http://localhost:8080</code>
</p>

<h2>Configuration et sécurité</h2>

<p>
Les variables d’environnement et les identifiants cloud
ne sont pas inclus dans le dépôt.
</p>

<p>
Un fichier <code>.env</code> et un fichier <code>profiles.yml</code> (dbt)
doivent être fournis localement pour exécuter le pipeline.
</p>

<h2>Exploitation analytique</h2>

<p>
Les données transformées et modélisées sont exploitées dans Power BI
afin de réaliser des analyses telles que :
</p>

<ul>
  <li>Analyse des prix de l’immobilier sur la région de Rouen</li>
  <li>Suivi de l’évolution des annonces dans le temps</li>
  <li>Comparaison par type de bien</li>
</ul>

<h2>Axes d’amélioration</h2>

<ul>
  <li>Mise en place de tests de qualité de données avec dbt</li>
  <li>Ajout de métriques de monitoring et d’alerting</li>
  <li>Intégration d’une CI/CD (GitHub Actions)</li>
  <li>Déploiement cloud complet</li>
</ul>

<p>
Projet réalisé dans un contexte pédagogique et professionnel,
orienté Data Engineering.
</p>

</body>
</html>