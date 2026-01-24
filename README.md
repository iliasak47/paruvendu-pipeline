# Projet Data Engineering – Pipeline Immobilier ParuVendu

## Sommaire
- [Contexte et Objectifs](#contexte-et-objectifs)
- [Périmètre Fonctionnel](#perimetre-fonctionnel)
- [Architecture Globale](#architecture-globale)
- [Stack Technique](#stack-technique)
- [Flux de Données & Médaillon](#flux-de-donnees-medaillon)
- [Stockage & Stratégie Analytique](#stockage-strategie-analytique)
- [Fiabilité & Orchestration](#fiabilite-orchestration)
- [Observabilité & Qualité](#observabilite-qualite)
- [Valeur Ajoutée & Compétences](#valeur-ajoutee-competences)
- [Axes d'Amélioration](#axes-amelioration)



<a id="contexte-et-objectifs"></a>
## Contexte et Objectifs
Ce projet illustre la conception d'un pipeline de données automatisé pour la collecte et l'analyse des annonces immobilières du site ParuVendu (région de Rouen). L'objectif est de transformer une donnée web brute en indicateurs décisionnels via une architecture moderne de Data Engineering.

Le système répond à deux impératifs majeurs :

- **Automatisation (Batch)** : Extraction hebdomadaire sans intervention humaine via un système d'orchestration robuste.
- **Analytique (Historique)** : Suivi de l'évolution des prix et de la persistance des annonces pour identifier les opportunités du marché.

L'architecture met l'accent sur la modularité, le traitement distribué avec PySpark et la modélisation rigoureuse avec dbt.

<a id="perimetre-fonctionnel"></a>
## Périmètre Fonctionnel

Le système assure les fonctions suivantes :

- **Ingestion Automatisée** : Scraping de pages HTML et stockage immuable sur AWS S3.
- **Traitement Distribué** : Nettoyage et enrichissement des données (extraction de prix, surface, ville via Regex) avec PySpark.
- **Historisation Temporelle** : Consolidation des snapshots pour permettre une analyse de tendance sur le long terme.
- **Modélisation métier** : Création de tables de faits et de dimensions (Marts) pour le reporting.

<a id="architecture-globale"></a>
## Architecture Globale
L'architecture suit un modèle de Data Lakehouse où AWS S3 sert de stockage central et AWS Athena de moteur de calcul SQL pour la couche finale.

**Graphe 1 : Vue d'ensemble des composants**  
```mermaid
flowchart LR
    subgraph Source[" Source Externe"]
        PV("Site Web ParuVendu<br/>(Immobilier Rouen)")
    end

    subgraph Docker[" Environnement Conteneurisé (Local)"]
        direction TB
        AF(["Apache Airflow 3.1.0<br/>(Orchestrateur)"])
        SP(["PySpark 3.5.2<br/>(Moteur de calcul)"])
        DBT(["dbt-core / Athena<br/>(Modélisation)"])
        
        AF -- "Déclenche" --> SP
        AF -- "Pilote" --> DBT
    end

    subgraph AWS["☁️ Services Cloud AWS"]
        direction TB
        S3[("Amazon S3<br/>(Data Lakehouse)")]
        ATH("Amazon Athena<br/>(Moteur SQL Serverless)")
        
        S3 -.-> ATH
    end

    subgraph Viz[" Restitutions"]
        PBI("Power BI<br/>(Reporting & KPIs)")
    end

    %% Flux de données principaux
    PV -- "Extraction (Requests / BS4)" --> AF
    SP -- "Écritures (Parquet)" --> S3
    DBT -- "Transformations / Tests" --> ATH
    ATH -- "Connexion Live" --> PBI

    %% Styling pour un look professionnel
    style PV fill:#f9f9f9,stroke:#333,stroke-width:2px
    style AF fill:#017cee,stroke:#fff,color:#fff
    style SP fill:#e25a1c,stroke:#fff,color:#fff
    style DBT fill:#ff694b,stroke:#fff,color:#fff
    style S3 fill:#ff9900,stroke:#fff,color:#fff
    style ATH fill:#ff9900,stroke:#fff,color:#fff
    style PBI fill:#f2c811,stroke:#333
```

### Structure du Projet
L’arborescence est segmentée par responsabilité technique :

```text
├── .github/workflows/ # CI/CD pour dbt
├── dags/
│   ├── scripts/ # Logique PySpark (Scrape, Parse, Transform, Historize)
│   ├── dbt/ # Modélisation SQL & Tests
│   └── paruvendu_pipeline.py # Orchestration Airflow
├── Dockerfile # Environnement Spark/Airflow/dbt
└── docker-compose.yaml # Infrastructure conteneurisée
```
<a id="stack-technique"></a>
## Stack Technique


Le choix technologique privilégie des outils standards du marché pour assurer la scalabilité :

- Orchestration : Apache Airflow 3.1.0.
- Traitement de Données : PySpark 3.5.2.
- Modélisation & T-ELT : dbt-core 1.10.13 & dbt-athena-community.
- Stockage Cloud : AWS S3 (Data Lake) & AWS Athena.
- Qualité & Tests : dbt_expectations.
- Infrastructure : Docker & GitHub Actions.

<a id="flux-de-donnees-medaillon"></a>
## Flux de Données & Médaillon


Le pipeline suit l'architecture Medallion, garantissant une traçabilité totale de la donnée.

**Graphe 2 : Cycle de vie de la donnée (Medallion)**  
```mermaid
graph TD
    %% Couche Raw
    subgraph Raw [" 1. Raw Layer (Bronze)"]
        direction TB
        R_Store[("<b>S3 Bucket: /raw/</b><br/>Fichiers HTML bruts<br/><i>(page_1.html, page_2.html...)</i>")]
        R_Script["<b>Script: S3_scrape_paruvendu.py</b><br/>Scraping via Requests"]
    end

    %% Couche Silver
    subgraph Silver [" 2. Silver Layer"]
        direction TB
        S_Script["<b>Script: S3_parse_paruvendu.py</b><br/>Parsing BeautifulSoup4"]
        S_Store[("<b>S3 Bucket: /silver/</b><br/>Fichier CSV structuré<br/><i>(YYYY-MM-DD.csv)</i>")]
    end

    %% Couche Gold
    subgraph Gold [" 3. Gold Layer"]
        direction TB
        G_Script["<b>Script: S3_transform_paruvendu.py</b><br/>Traitement PySpark"]
        G_Ops["<b>Transformations :</b><br/>- Regex : Prix, Surface, Ville<br/>- Calcul : Prix au m²<br/>- Typage des colonnes"]
        G_Store[("<b>S3 Bucket: /gold/</b><br/>Fichier Parquet nettoyé<br/><i>(YYYY-MM-DD_clean.parquet)</i>")]
    end

    %% Couche History
    subgraph History [" 4. History Layer"]
        direction TB
        H_Script["<b>Script: S3_historize_paruvendu.py</b><br/>Fusion PySpark (Union)"]
        H_Store[("<b>S3 Bucket: /history/</b><br/>Dataset final consolidé<br/><i>(history.parquet)</i>")]
    end

    %% Flux de données
    R_Script --> R_Store
    R_Store --> S_Script
    S_Script --> S_Store
    S_Store --> G_Script
    G_Script --> G_Ops
    G_Ops --> G_Store
    G_Store --> H_Script
    H_Script --> H_Store

    %% Styles Professionnels
    style R_Store fill:#f3f4f6,stroke:#374151
    style S_Store fill:#e0f2fe,stroke:#0369a1
    style G_Store fill:#f0fdf4,stroke:#166534
    style H_Store fill:#fff7ed,stroke:#c2410c

    style G_Ops fill:#fefce8,stroke:#a16207,stroke-dasharray: 2 2
    
    style R_Script fill:#ffffff,stroke:#374151
    style S_Script fill:#ffffff,stroke:#374151
    style G_Script fill:#ffffff,stroke:#374151
    style H_Script fill:#ffffff,stroke:#374151
```

Le flux se décompose ainsi :

- Raw (Bronze) : Stockage des fichiers HTML bruts datés.
- Silver : Conversion en CSV structuré.
- Gold : Nettoyage PySpark (Regex, typage, calcul prix/m²).
- History : Fusion des fichiers Gold en un historique Parquet unique sur S3.

<a id="stockage-strategie-analytique"></a>
## Stockage & Stratégie Analytique


L'usage de dbt sur Athena permet de découpler le stockage de la logique métier.

- Enrichissement (Seeds) : Intégration de référentiels pour mapper le nombre de pièces aux typologies de logement (T2, T3, etc.).
- Analyse de Persistance : Mesure de la durée de présence des annonces pour détecter les biens à forte rotation.

**Graphe 3 : Couches de modélisation dbt**  
```mermaid
graph TD
    subgraph Storage [" Couche Stockage (AWS S3)"]
        S3_Hist[("<b>paruvendu_history</b><br/>(history.parquet)")]
    end

    subgraph dbt_Staging [" Couche Staging (Vues)"]
        STG["<b>stg_paruvendu</b><br/>Nettoyage, Renommage,<br/>Typage des données"]
    end

    subgraph dbt_Enrichment [" Enrichissement (Seeds)"]
        SEED["<b>ref_typologie_logement</b><br/>Mapping : nb_pieces → Label<br/><i>(Studio, T2, T3...)</i>"]
    end

    subgraph dbt_Marts [" Couche Analytique (Tables Marts)"]
        direction TB
        M1["<b>mart_typologie_logement</b><br/>Analyse par type de bien<br/><i>(Join Staging + Seed)</i>"]
        M2["<b>mart_evolution_prix</b><br/>Tendances hebdomadaires"]
        M3["<b>mart_top_annonces</b><br/>20% des meilleures opportunités<br/><i>(Percentiles)</i>"]
    end

    subgraph dbt_Analyses [" Analyses Ad-hoc"]
        PERSIST["<b>persistence_annonces.sql</b><br/>Calcul de la durée de visibilité<br/>(Analyse de rotation)"]
    end

    %% Flux de données
    S3_Hist --> STG
    STG --> M1
    STG --> M2
    STG --> M3
    STG --> PERSIST
    SEED --> M1

    %% Restitution
    M1 & M2 & M3 -.-> PBI[" Reporting Power BI"]

    %% Styles Professionnels
    style S3_Hist fill:#f3f4f6,stroke:#374151
    style STG fill:#e0f2fe,stroke:#0369a1
    style SEED fill:#fefce8,stroke:#a16207,stroke-dasharray: 5 5
    
    style M1 fill:#f0fdf4,stroke:#166534
    style M2 fill:#f0fdf4,stroke:#166534
    style M3 fill:#f0fdf4,stroke:#166534
    
    style PERSIST fill:#faf5ff,stroke:#6b21a8,stroke-width:2px
    style PBI fill:#fef9c3,stroke:#a16207,stroke-width:2px

    %% Conteneurs
    style dbt_Staging fill:#f8fafc,stroke:#475569
    style dbt_Marts fill:#f8fafc,stroke:#475569
    style dbt_Enrichment fill:#f8fafc,stroke:#475569
```

<a id="fiabilite-orchestration"></a>
## Fiabilité & Orchestration


Pour garantir la résilience du pipeline, chaque étape est isolée et monitorée.

- Idempotence : Les scripts Spark sont conçus pour traiter les partitions quotidiennes de manière indépendante.
- CI/CD : Un workflow GitHub Actions valide chaque modification de modèle dbt et exécute les tests avant le déploiement.

**Graphe 4 : Workflow d'Orchestration (DAG)**  
```mermaid
graph TD
    subgraph CI[" CI/CD - Validation GitHub "]
        direction TB
        A1["<b>Push / Pull Request</b><br/>(main branch)"]
        A2["<b>Pipeline dbt CI</b><br/><i>dbt-ci.yml</i>"]
        A3["<b>Validation dbt build</b><br/>(Compilation & Tests CI)"]
        
        A1 --> A2 --> A3
    end

    A3 -. "Déploiement & Exécution" .-> T1

    subgraph DAG ["Orchestration Airflow DAG"]
        direction TB
        
        T1["<b>Task: scrape</b><br/><i>S3_scrape_paruvendu.py</i><br/> Destination : s3://.../raw/"]
        
        T2["<b>Task: parse</b><br/><i>S3_parse_paruvendu.py</i><br/> Destination : s3://.../silver/"]
        
        T3["<b>Task: transform_gold</b><br/><i>S3_transform_paruvendu.py</i><br/> Spark Clean (Idempotent) → /gold/"]
        
        T4["<b>Task: historize</b><br/><i>S3_historize_paruvendu.py</i><br/> Archive → /history/history.parquet"]

        T5["<b>Task: dbt_freshness</b><br/>Contrôle SLA Source<br/>(Max 48h)"]
        
        T6["<b>Task: dbt_run</b><br/>Génération des Marts<br/>(Athena Models)"]
        
        T7["<b>Task: dbt_test</b><br/>Qualité finale<br/>(dbt_expectations)"]

        T1 --> T2 --> T3 --> T4 --> T5 --> T6 --> T7
    end

    %% Styles de différenciation
    style A3 fill:#dcfce7,stroke:#166534,stroke-width:2px
    style T3 fill:#fff7ed,stroke:#c2410c,stroke-width:2px
    style T4 fill:#fff7ed,stroke:#c2410c,stroke-width:2px
    style T7 fill:#fef2f2,stroke:#b91c1c,stroke-width:2px
    
    %% Styles des conteneurs
    style CI fill:#f1f5f9,stroke:#64748b,stroke-dasharray: 5 5
    style DAG fill:#f8fafc,stroke:#475569
```
<a id="observabilite-qualite"></a>
## Observabilité & Qualité


La confiance dans la donnée est assurée par deux mécanismes clés :

- Freshness dbt : Contrôle automatique que les données sources sur S3 ne datent pas de plus de 48h.
- Tests d'Intégrité : Validation via dbt_expectations que les prix sont positifs, les surfaces réalistes et les identifiants uniques.

<a id="valeur-ajoutee-competences"></a>
## Valeur Ajoutée & Compétences


Ce projet démontre une expertise sur l'ensemble du cycle de vie de la donnée :

- ✅ Architecture Cloud : Gestion de Data Lake sur AWS S3.
- ✅ Big Data Processing : Maîtrise de PySpark pour le nettoyage complexe de données non structurées.
- ✅ Modern Data Stack : Utilisation intensive de dbt pour la gouvernance et la modélisation.
- ✅ DevOps for Data : Automatisation via Docker et GitHub Actions.

<a id="axes-amelioration"></a>
## Axes d'Amélioration


- Alerting : Intégration de notifications Slack/Email sur échec de tâche Airflow.
- Schema Registry : Utilisation de formats comme Glue Schema Registry pour valider la structure en entrée.
- Scalabilité Cloud : Migration vers AWS MWAA pour un Airflow managé.
