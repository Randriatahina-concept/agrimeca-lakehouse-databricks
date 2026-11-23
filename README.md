# AgriMeca Lakehouse — migration ERP vers SAP sur Azure Databricks

Plateforme de données de bout en bout pour un industriel fictif, AgriMeca, en pleine **migration de son ERP historique vers SAP S/4HANA** : ingestion de sources hétérogènes avec **Azure Data Factory** dans un data lake **ADLS Gen2**, architecture **médaillon** sur **Azure Databricks / Unity Catalog**, transformations et tests **dbt**, restitution **Power BI** et cas d'usage **IA**.

![Architecture des données AgriMeca : sources, Azure Data Factory, ADLS Gen2, médaillon Databricks et dbt, restitution](docs/images/architecture_v3.png)

> Projet personnel de portfolio. L'entreprise, ses systèmes et ses données sont simulés ; le scénario s'inspire d'une migration d'ERP vécue dans l'industrie, sans en reprendre aucun nom, aucun format de code ni aucun chiffre. Les volumes, les contraintes techniques et les défauts de données sont réalistes. Les chiffres présentés comme **mesurés** l'ont été sur les données du projet ; les autres sont des volumes cibles.

---

## 📊 Problématique

**Contexte** — AgriMeca conçoit et fabrique des **machines agricoles** (travail du sol, semis, fertilisation, protection des cultures, récolte fourragère, élevage) et vend des pièces de rechange à un réseau de concessionnaires. Ses usines assemblent les machines à partir de pièces achetées auprès de milliers de fournisseurs ; une pièce en retard peut arrêter une ligne d'assemblage en pleine saison.

Début 2025, AgriMeca a **remplacé son ERP historique par SAP S/4HANA**, en une bascule unique (*big bang*) un week-end de janvier, suivie de trois mois pendant lesquels les deux systèmes ont fonctionné en parallèle. Depuis, la direction ne peut plus répondre à des questions simples sur une période qui chevauche la bascule :

- **Un même article porte deux codes.** Avant la bascule, il s'appelle par exemple `A1234567` dans l'ERP historique ; après, il a un numéro SAP. Les commandes d'avant ne se rattachent donc pas aux articles d'après, et aucun indicateur ne peut couvrir plusieurs années d'affilée.
- **La table de transcodification est le seul lien entre les deux mondes.** Tenue par l'équipe migration dans un tableur, livrée chaque semaine, elle indique pour chaque ancien article son statut (repris tel quel, remplacé par un autre, abandonné, ou pas encore décidé) et son code SAP. Elle **évolue dans le temps** : il faut savoir quel était le statut d'un article au jour de la bascule, et non seulement aujourd'hui.
- **Les remplacements s'enchaînent** : un article A remplacé par B, lui-même remplacé par C, et parfois une boucle A → B → A due à une erreur de saisie.
- **Les deux systèmes ne se ressemblent pas** : exports CSV au format daté (encodage Windows-1252, décimales à virgule, dates `jj/mm/aaaa`) côté historique, tables SAP côté nouveau système, hiérarchies produits qui ne se correspondent pas une pour une.
- **Pendant le recouvrement, des commandes ont été saisies deux fois**, dans les deux systèmes ; les commandes ouvertes reprises dans SAP portent la date de bascule au lieu de leur vraie date, ce qui fausse les délais fournisseurs.
- **Les machines vendues envoient leur télémétrie** (heures moteur, températures, alarmes) : environ **1,2 milliard de mesures** sur 4 ans, que les boîtiers identifient avec l'ancien ou le nouveau code article selon leur date de vente.

**Constats déjà mesurés sur les achats** (version 2 du projet, avant l'ajout de SAP)

- **37 % des lignes de commande** sont en attente de livraison, sans vue consolidée de celles qui sont déjà en retard.
- Les achats sont libellés en **7 devises** : impossible de comparer les fournisseurs sans conversion.
- Les données de l'ERP sont **peu fiables** : 10 % de dates hors format, 2 % de devises manquantes, 1 % de quantités aberrantes, environ 8 % de fournisseurs en double.
- **0,5 % des lignes représentaient 84 % des montants d'achat** : sans contrôle qualité, les dépenses seraient surestimées d'un facteur 6.
- **19 % des lignes appartiennent à des commandes annulées** (5,1 Md€) : comptées comme des achats, elles fausseraient les dépenses d'autant.
- Une fois les données fiabilisées : **OTIF fournisseurs de 60,5 %**. Une livraison sur trois arrive en retard, une sur dix est incomplète.

**Objectif** — Construire une plateforme fiable et industrialisée qui :
- rassemble l'ERP historique, SAP, la transcodification, les API et la télémétrie dans un data lake unique ;
- **traduit les anciennes commandes dans le référentiel SAP** grâce à la transcodification historisée et à la résolution des chaînes de remplacement, pour obtenir **quatre ans d'historique continu sur une seule clé article** ;
- supprime les doublons du recouvrement, met en quarantaine ce qui ne peut pas être rattaché au lieu de le perdre, et protège les données personnelles ;
- calcule les indicateurs de pilotage des achats (fiabilité des fournisseurs, délais, dépenses) **et ceux de la migration** (articles non repris et chiffre d'affaires associé, stock non transcodé, commandes en double, délais avant et après la bascule).

## 🗂️ Sources de données

| Source | Contenu | Volume | Ingestion |
| --- | --- | --- | --- |
| **ERP historique** (exports CSV mensuels) | articles, fournisseurs, clients, commandes d'achat et de vente, mouvements et stock de clôture, jusqu'en mars 2025 | ≈ 56 millions de lignes (cible) | ADF, copie binaire → ADLS |
| **Transcodification** (CSV hebdomadaire) | ancien code article → code SAP, statut, date de décision ; 52 versions | ≈ 10 millions de lignes (cible) | ADF, copie binaire → ADLS |
| **SAP S/4HANA** (Azure SQL Database) | articles, fournisseurs, concessionnaires, commandes d'achat et de vente, mouvements, à partir de janvier 2025 | ≈ 67 millions de lignes (cible) | ADF, incrémental par filigrane → ADLS `csv.gz` |
| **Télémétrie machines** (fichiers `csv.gz` quotidiens) | mesures des machines vendues | ≈ **1,2 milliard de lignes** (cible) | ADF → ADLS |
| **API ERP AgriMeca** (FastAPI sur Azure Container Apps) | taux de change, qualité fournisseurs | — | ADF REST (OAuth, pagination par curseur) → ADLS |
| **API de la Banque centrale européenne** | taux de change de référence quotidiens | 6 devises | ADF REST → ADLS |

Toutes les sources arrivent dans une **zone de dépôt ADLS unique**, lue par Auto Loader dans le Bronze. La version 2, construite sur l'API ERP seule (1,3 million de commandes, **5,2 millions de lignes** mesurées), est dans le dépôt [agrimeca-supplychain-de](https://github.com/Randriatahina-concept/agrimeca-supplychain-de) : ses règles de calcul de l'OTIF et des dépenses sont réutilisées sur les achats unifiés.

## 🛠️ Stack

| Outil | Rôle |
| --- | --- |
| **Azure Data Factory** | ingestion de toutes les sources (fichiers, Azure SQL, API REST), identité managée |
| **ADLS Gen2** | zone de dépôt `csv.gz`, une seule copie brute |
| **Azure SQL Database** | base SAP S/4HANA simulée, authentification Entra uniquement |
| **Azure Key Vault** | secrets de l'API, sel de hachage des données personnelles |
| **Databricks** (Unity Catalog, Delta Lake, Auto Loader) | Bronze incrémental, gouvernance, calcul serverless |
| **dbt Core** (`dbt-databricks`) | Silver et Gold, tests, documentation |
| **Microsoft Fabric** | génération des données simulées (Spark) ; lecture du Gold par catalogue mirroré |
| **Power BI** | tableaux de bord achats, migration, continuité |
| **GitHub** | code, pipelines ADF, bundle Databricks, CI |

## 🏗️ Architecture (médaillon)

| Couche | Contenu | Outil |
| --- | --- | --- |
| **Dépôt** | fichiers livrés par ADF dans ADLS, par source, table et date de chargement ; jamais modifiés | ADF, ADLS |
| 🥉 **Bronze** | une table par table source, tout en texte, colonnes de traçabilité (fichier, date de chargement) ; un notebook générique piloté par une table de configuration | Auto Loader |
| 🥈 **Silver** | historique et SAP typés et nettoyés séparément ; données personnelles pseudonymisées ; transcodification historisée ; chaînes de remplacement résolues ; couche unifiée sur une seule clé article | dbt |
| 🥇 **Gold** | dimensions conformes (articles avec les deux hiérarchies, fournisseurs, concessionnaires, usines, dates avant / pendant / après la bascule, machines), faits d'achats et de ventes, KPI de migration, agrégats de télémétrie | dbt |

Le catalogue Unity Catalog `ct_agrimeca` porte les schémas `landing`, `bronze`, `silver` et `gold`. La landing est un volume externe sur ADLS ; les tables sont managées.

## ✅ Qualité, sécurité et gouvernance

- **Tests dbt à chaque construction** : unicité, valeurs obligatoires, relations entre faits et dimensions, valeurs admises ; les incohérences connues sont signalées (`warn`) sans bloquer.
- **Quarantaine** : chaque ligne qui ne peut pas être rattachée (article non transcodé, clé orpheline) porte un motif et reste consultable ; rien n'est supprimé en silence.
- **Données personnelles** : noms et téléphones supprimés, e-mails réduits à leur domaine, identifiants d'utilisateurs hachés avec un sel propre à chaque environnement. C'est une **pseudonymisation**, pas une anonymisation au sens du RGPD.
- **Aucun secret** dans le code ni dans Git : identités managées, Key Vault, authentification Entra uniquement sur Azure SQL.
- **Lignage** de bout en bout dans Unity Catalog et dbt.

## 🎯 Indicateurs

**Migration**

| Indicateur | Question |
| --- | --- |
| Taux de transcodification par version | le chantier était-il prêt au jour de la bascule ? |
| Chiffre d'affaires des articles non repris | quel chiffre d'affaires risquait-on de perdre, et quels articles prioriser ? |
| Stock non transcodé | combien vaut le stock de clôture d'articles jamais repris dans SAP ? |
| Réconciliation des stocks | le stock repris dans SAP correspond-il à l'arrêté de l'ERP historique ? |
| Commandes en double | combien de commandes ont été saisies deux fois pendant le recouvrement ? |
| Délais fournisseurs avant et après | la bascule a-t-elle dégradé l'approvisionnement ? |

**Achats** : OTIF, délais, dépenses, classification ABC, retards en cours, calculés sur quatre ans d'historique unifié avec les règles validées en version 2 (OTIF mesuré à 60,5 % sur l'API ERP seule).

**Pipeline** : fraîcheur de chaque source, tests passants, durée et coût d'une exécution quotidienne, à mesurer à chaque palier de volume.

## 🚀 Démarrer

```bash
git clone https://github.com/Randriatahina-concept/agrimeca-lakehouse-databricks.git
cd agrimeca-lakehouse-databricks
```

Prérequis Azure (détaillés dans la documentation technique) : un compte ADLS Gen2, une Data Factory, une base Azure SQL, un workspace Databricks avec Unity Catalog. Les commandes de génération, d'ingestion et de construction seront ajoutées au fil des étapes.

## 📁 Structure du dépôt

```text
agrimeca-lakehouse-databricks/
├── adf/                # pipelines Azure Data Factory (JSON, intégration Git d'ADF)
├── generator/          # générateur des données simulées (Fabric)
├── notebooks/bronze/   # notebook Auto Loader générique et sa configuration
├── dbt/                # projet dbt : Silver et Gold
├── bundle/             # Job Databricks décrit en bundle (databricks.yml)
├── tools/              # utilitaires, dont le contrôle de non-ressemblance
├── tests/              # tests pytest
└── docs/images/        # schéma d'architecture
```

## 🗺️ Feuille de route

| Étape | Statut |
| --- | --- |
| Infrastructure Azure : ADLS Gen2, Data Factory, Unity Catalog (credential, external locations, catalogue, volumes) | fait |
| Base Azure SQL (SAP simulé), Entra uniquement | en cours |
| Générateur de données à petite échelle (≈ 17 millions de lignes) | à venir |
| Ingestion ADF vers la landing `csv.gz` (fichiers, SAP, API) | à venir |
| Bronze : Auto Loader générique | à venir |
| Silver : transcodification historisée, chaînes de remplacement, couche unifiée, pseudonymisation | à venir |
| Gold : modèle en étoile, KPI de migration, agrégats de télémétrie | à venir |
| Orchestration : pipeline ADF quotidien → Job Databricks, rattrapage rejouable | à venir |
| Montée en volume jusqu'à 1,7 milliard de lignes, mesure des coûts | à venir |
| CI, Power BI, comparaison avec la version Fabric | à venir |

## 📚 Documentation

- **Dossier technique** : configurations, code commenté, décisions, pièges rencontrés et résultats de chaque étape.
- **Dictionnaire des données** : chaque table et chaque colonne, valeurs possibles et défauts connus.
- **Version 2** : [agrimeca-supplychain-de](https://github.com/Randriatahina-concept/agrimeca-supplychain-de), API ERP, Gold achats et OTIF mesuré.

## Auteur

Charles Randriatahina, Data Engineer
