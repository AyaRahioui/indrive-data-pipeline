# Pipeline de courses InDrive (version légère)

Pipeline de données à trois couches (**raw → staging → mart**) pour une plateforme de VTC fictive opérant à Casablanca. Orchestré par **Airflow**, stocké dans **Postgres**, entièrement exécuté avec **Docker**.

Projet réalisé dans le cadre du module 3 « Fondamentaux de l'ingénierie des données » (Data Maroc Bootcamps).

---

## Architecture

```
                     ┌────────────────────┐
 3 CSV (batch) ────► │  2_batch (@daily)  │──┐
 drivers, vehicles,  └────────────────────┘  │
 zones                                       ▼
                                          ┌─────┐   ┌─────────────────────┐   ┌─────────────┐
 API Mockaroo ─────► ┌────────────────────┐│ raw │──►│ 3_staging (*/2 min) │──►│ 4_marts     │
 (streaming)         │ 1_streaming        ││     │   │ nettoyage           │   │ (déclenché) │
                     │ (chaque minute)    │└─────┘   └─────────────────────┘   └─────────────┘
                     └────────────────────┘
```

| DAG | Planification | Rôle |
|---|---|---|
| `1_streaming` | chaque minute | Récupère des événements de course (API Mockaroo) et les **ajoute** à `raw.rides` |
| `2_batch` | quotidien | **Tronque et recharge** `raw.drivers`, `raw.vehicles`, `raw.zones` depuis les CSV |
| `3_staging` | toutes les 2 min | Attend que 1 et 2 aient réussi une fois, construit les 4 tables `staging.*` en parallèle, puis déclenche `4_marts` |
| `4_marts` | aucune (`schedule=None`) | Construit `mart.rides`, puis `mart.revenue_by_zone` et `mart.driver_leaderboard` en parallèle |

### Sources de données

- **Batch** (référentiels, dimensions) : `data/drivers.csv`, `data/vehicles.csv`, `data/zones.csv`.
- **Streaming** (fait) : événements de course. Une ligne de `raw.rides` = un événement de course.
  - Source : API Mockaroo (schéma `indrive_rides`).
  - Repli automatique sur `data/mockaroo_rides.json` si l'API est indisponible ou si le quota est atteint.
  - Environ 5 % des courses reçoivent volontairement un `driver_id` inexistant (999) et environ 3 % sont dupliquées, pour tester le nettoyage du staging.

### Modèle de données

| Schéma | Tables | Contenu |
|---|---|---|
| `raw` | `drivers`, `vehicles`, `zones`, `rides` | Copie brute des sources |
| `staging` | `drivers`, `vehicles`, `zones`, `rides` | Colonnes utiles uniquement, textes nettoyés (`TRIM`), doublons supprimés, courses invalides retirées |
| `mart` | `rides`, `revenue_by_zone`, `driver_leaderboard` | Tables prêtes pour l'analyse |

**Règles de nettoyage de `staging.rides`** : suppression des doublons sur `ride_id`, retrait des courses avec `fare_amount <= 0`, retrait des courses dont le `driver_id` n'existe pas dans `raw.drivers`.

**Grain de `mart.rides`** : une ligne = une course valide, identifiée par `ride_id`, avec le nom du chauffeur, sa note et la zone de départ.

### Choix de conception

- **ELT** : les données sont chargées brutes dans Postgres, puis transformées en SQL.
- **Idempotence** : chaque table staging/mart est reconstruite par `DROP TABLE IF EXISTS` + `CREATE TABLE AS`. Relancer un DAG ne crée jamais de doublon.
- **Batch vs streaming** : les référentiels sont rechargés entièrement (`TRUNCATE` + `COPY` dans une transaction) ; les courses sont ajoutées (`INSERT`) car un flux s'accumule.
- **Pas de donnée sensible en staging** : `phone` et `plate_number` ne sont pas conservés.

---

## Structure du projet

```
pipeline/
├── docker-compose.yml
├── .env                         # clé Mockaroo (non versionné)
├── README.md
├── WORKSHEET.md                 # réponses aux questions Q1-Q6
├── data/
│   ├── drivers.csv
│   ├── vehicles.csv
│   ├── zones.csv
│   └── mockaroo_rides.json      # repli du streaming
├── postgres/
│   └── init.sql                 # schémas raw/staging/mart + tables raw
├── dags/
│   ├── db.py                    # connexion + exécution d'un fichier SQL
│   ├── streaming_dag.py         # 1_streaming
│   ├── batch_dag.py             # 2_batch
│   ├── staging_dag.py           # 3_staging
│   └── marts_dag.py             # 4_marts
└── sql/
    ├── staging/                 # drivers.sql, vehicles.sql, zones.sql, rides.sql
    └── mart/                    # rides.sql, revenue_by_zone.sql, driver_leaderboard.sql
```

---

## Prérequis

- Docker Desktop (avec Docker Compose)
- Un compte Mockaroo (gratuit) pour la clé API
- Sous Windows, il est conseillé de placer le projet hors de OneDrive (par exemple `C:\pipeline`), car les montages de volumes y sont parfois instables

---

## Installation et démarrage

### 1. Préparer les données Mockaroo

1. Sur mockaroo.com, créez le schéma `indrive_rides` avec ces champs :

   | Champ | Type | Réglage |
   |---|---|---|
   | `ride_id` | GUID | |
   | `driver_id` | Number | 1 à 8, 0 décimale |
   | `pickup_zone_id` | Number | 1 à 8, 0 décimale |
   | `dropoff_zone_id` | Number | 1 à 8, 0 décimale |
   | `requested_at` | Datetime | |
   | `distance_km` | Number | 1 à 30, 1 décimale |
   | `fare_amount` | Number | -5 à 120, 2 décimales (les valeurs ≤ 0 testent le filtre) |

2. Cliquez sur **Save Changes**, puis générez 1000 lignes au format **JSON** (`Generate Data`). Renommez le fichier en `mockaroo_rides.json` et placez-le dans `data/`.
3. Récupérez votre clé API dans les paramètres de votre compte Mockaroo.
4. Testez l'API avant de continuer :

   ```bash
   curl "https://api.mockaroo.com/api/generate.json?key=VOTRE_CLE&schema=indrive_rides&count=5"
   ```

### 2. Configurer la clé

Créez un fichier `.env` à côté de `docker-compose.yml` :

```
MOCKAROO_KEY=VOTRE_CLE
```

Le compose doit contenir, dans `environment:` du service `airflow` :

```yaml
MOCKAROO_KEY: ${MOCKAROO_KEY:-}
```

> Ne publiez jamais `.env` : ajoutez-le à `.gitignore`.

### 3. Lancer les services

```bash
docker compose up -d
docker compose logs -f airflow     # attendre "Airflow is ready" (1 à 2 minutes)
```

> `postgres/init.sql` ne s'exécute qu'au **premier démarrage** d'un volume vide. Après l'avoir modifié, repartez de zéro avec `docker compose down -v`.

### 4. Se connecter

| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8081 | `admin` / mot de passe généré (voir ci-dessous) |
| Adminer | http://localhost:8085 | Système : PostgreSQL, serveur : `postgres`, utilisateur / mot de passe / base : `indrive` |

Mot de passe admin d'Airflow :

```bash
docker compose logs airflow | grep -i password
```

> **Git Bash sous Windows** : les chemins commençant par `/` sont réécrits. Pour `docker compose exec`, ajoutez `MSYS_NO_PATHCONV=1` devant la commande, par exemple :
> `MSYS_NO_PATHCONV=1 docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt`

---

## Vérifier que ça fonctionne

1. Dans Airflow, les 4 DAGs sont visibles et passent au vert (`3_staging` attend d'abord que `1_streaming` et `2_batch` aient réussi une fois).
2. Dans les logs de `1_streaming` → `load_rides`, la ligne `SOURCE=API Mockaroo` confirme l'usage de l'API réelle.
3. Dans Adminer (**SQL command**) :

   ```sql
   SELECT count(*) FROM raw.drivers;          -- 8
   SELECT count(*) FROM raw.rides;            -- augmente chaque minute
   SELECT count(*) FROM staging.rides;        -- inférieur à raw.rides
   SELECT * FROM mart.revenue_by_zone;
   SELECT * FROM mart.driver_leaderboard;
   ```

4. Comptage de ce qui est filtré en staging :

   ```sql
   SELECT
     (SELECT count(*) FROM raw.rides)                                  AS raw_rows,
     (SELECT count(*) FROM staging.rides)                              AS staging_rows,
     (SELECT count(*) FROM raw.rides WHERE fare_amount <= 0)           AS fare_invalide,
     (SELECT count(*) FROM raw.rides
        WHERE driver_id NOT IN (SELECT driver_id FROM raw.drivers))    AS chauffeur_inconnu,
     (SELECT count(*) - count(DISTINCT ride_id) FROM raw.rides)        AS doublons;
   ```

5. **Test d'idempotence** : déclenchez `3_staging` 2 ou 3 fois à la main, puis vérifiez l'absence de doublon (les deux requêtes doivent renvoyer 0 ligne) :

   ```sql
   SELECT ride_id, count(*) FROM staging.rides GROUP BY ride_id HAVING count(*) > 1;
   SELECT ride_id, count(*) FROM mart.rides    GROUP BY ride_id HAVING count(*) > 1;
   ```

---

## Limites connues

- **Quota Mockaroo** : le plan gratuit autorise 200 requêtes par jour, alors que `1_streaming` en fait une par minute. Après environ 3 h 20, l'API répond 429 et le DAG bascule automatiquement sur `data/mockaroo_rides.json`. Le pipeline continue de tourner.
- **Exécution séquentielle** : Airflow en mode `standalone` utilise SQLite et un exécuteur séquentiel. Les 4 tâches staging sont déclarées en parallèle dans le DAG, mais exécutées l'une après l'autre. Pour un vrai parallélisme, il faudrait un exécuteur `LocalExecutor` avec une base Postgres dédiée aux métadonnées d'Airflow.
- **Environnement de démonstration** : identifiants simples (`indrive` / `indrive`) et Postgres non exposé à l'extérieur du réseau Docker. Ne pas utiliser tel quel en production.

---

## Dépannage

| Symptôme | Cause probable | Solution |
|---|---|---|
| Un DAG n'apparaît pas | Erreur d'import Python ou fichier mal placé | `docker compose exec airflow airflow dags list-import-errors` |
| `relation "raw.xxx" does not exist` | `init.sql` non exécuté (mauvais nom de dossier ou volume ancien) | Vérifier que le dossier s'appelle `postgres`, puis `docker compose down -v` et `docker compose up -d` |
| `3_staging` reste en attente | `1_streaming` ou `2_batch` n'a jamais réussi | Ouvrir leurs logs, corriger, relancer |
| `2_batch` échoue sur `COPY` | Colonnes de la table différentes de celles du CSV | Aligner `init.sql` sur l'ordre des colonnes du CSV |
| Logs : `repli sur le fichier` | Clé absente, schéma mal nommé ou quota dépassé | Vérifier `.env`, le nom `indrive_rides` et le test `curl` |
| `No such file or directory` avec `C:/Program Files/Git/...` | Git Bash réécrit les chemins | Préfixer par `MSYS_NO_PATHCONV=1` |
| Une tâche est rouge | Erreur SQL ou Python | Ouvrir la tâche, onglet **Logs** |

---

## Commandes utiles

```bash
docker compose up -d            # démarrer
docker compose logs -f airflow  # suivre les logs
docker compose down             # arrêter (les données sont conservées)
docker compose down -v          # arrêter et tout effacer (rejoue init.sql)
```
