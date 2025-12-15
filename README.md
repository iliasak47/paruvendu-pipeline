<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <title>Projet Data Engineering – Pipeline Immobilier ParuVendu</title>
  <style>
    body {
      font-family: Arial, Helvetica, sans-serif;
      line-height: 1.6;
      max-width: 1000px;
      margin: auto;
      padding: 24px;
      color: #1f1f1f;
      background-color: #ffffff;
    }
    h1, h2, h3 {
      color: #1a2b3c;
      margin-top: 32px;
    }
    pre, code {
      background-color: #f6f8fa;
      padding: 12px;
      display: block;
      overflow-x: auto;
      font-size: 0.95em;
    }
    ul {
      margin-left: 20px;
    }
    .stack span {
      display: inline-block;
      margin: 4px 6px 4px 0;
      padding: 4px 10px;
      border: 1px solid #ccc;
      border-radius: 4px;
      font-size: 0.85em;
    }
  </style>
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
