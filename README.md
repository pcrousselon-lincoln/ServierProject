# Servier Test : Data Engineer

## 1.Les Données 

Cet exercice nous demande de charger un certain nombre de données de format et de type 
différent. 
Une anaylse primaire des données nous permettra de mieux antinciper les besoins et le travail
à fournir pour charger et transformer ces données. 

### Drugs
Il s'agit d'un simple fichier .csv de deux colonnes. 
Il n'y a pas de problèmes de qualité (doublons ou données manquantes). Cela peut ce comprendre puisqu'il 
doit s'agir d'une table de référence qui subit de nombreux test. 
On note cependant que tous les noms des drugs sont en _majuscules_.

### Pubmeds 
Ces données sont séparées en 2 fichiers 1 csv et 1 json. 
L'idée est donc de créer une fonction qui peut chercher plusieurs type de documents dans le datalake 
et tout charger dans une seule sortie. 
Si on analyse chaque fichier, on peut voire : 
- pubmed.csv :
  - tous les champs sont renseignés mais on observe un problème de qualité pour la gestion de la date. En effet,
      la plupart des lignes sont au format dd/mm/yyyy mais un ligne est au format yyyy-mm-dd. Il faudra alors penser à une 
      fonction de prétraitement pour ne pas avoir d'erreur lors de la conversion en type date. 
- pubmed.json : 
  - La première chose qu'on remarque en ouvrant le fichier dans l'éditeur c'est que le fichier est "cassé". Il ne 
  pas le standard json puisqu'il y a une ',' à la fin du fichier ce qui est interdit et peut aussi poser des problèmes
  si on essaie de le charger directement. Il faudra penser à une fonction de prétratiement ou de contournement pour le charger.
  - La seconde chose que l'on observe est que la 5 ieme donnée est très anormale. Déjà elle n'a pas d'id, ensuite elle 
  semble reprendre le titre de la ligne 12 en ajoutant des noms de médicaments. C'est un genre de donnée corrumpue que l'on 
  peut trouver dans des données sources externes et qu'il faut gérer en tant que DE mais en lien avec le metier. En effet,
  de mon point de vue, c'est le responsable fonctionnel de la donnée qui doit donner son avis sur la marche à suivre dans ce 
  cas de figure. Il imposera les règles de pré traitements pour savoir si on doit garder ou supprimer ce genre de données
  corrompues. Même si cela rique de fausser nos résultats, j'ai pris le parti de ne pas supprimer cette ligne. 
- Pour les 2 fichiers : La casse n'est pas régulière et dépend de chaque titre. Pour ce projet seul le nom du médicament
    nous intéresse, et on peut trouver : tout minuscule, touts majuscule ou 1 majuscule. Il faudra donc y faire attention 
    pour synchroniser la jointure avec la table drugs

### Clincal Trials
Il s'agit d'un unique fichier csv mais au premier regard, on voit qu'il regroupe tous les problèmes de data quality qu'on
peut espérer rencontrer : 
- Il y a des données manquantes (id et titre)
- Il y a des caractères mal encodés dans le titre et le journal (ex; "\xc3\xb1") 
- Il n'y a pas de règle pour la casse sur les titres 
- Il y a un problème de typage pour la date soit "dd/mm/yyyy" soit "dd month yyyy"
- Il y a des caractères spéciaux (ô) qui vont poser problèmes si on essaye de nettoyer les caractères mal encodés. 

### Conclusion sur les données 
On a observé de nombreux type de problèmes de data quality sur les données. 
Il faudra donc faire une ou plusieures étapes de prétraitement lors du chargement des données pour ne pas retrouver ces  
erreurs dans nos données. 
De mon expérience, le plus courant est de sauvegarder les tables nettoyées dans une zone appelée "silver", contraitement 
aux données brutes appelées "raw" car elles peuvent servir à plusieurs traitements différents et cela evitera de refaire
plusieurs fois tous les calculs de pré traitement. 
Dans notre cas, le minimum pour avancer est de synchroniser la casse entre les titres et les noms des drugs pour faire
une jointure fonctionnelle. 

## Travail à réaliser 
Le besoin exprimé ici est de liée les drugs à leurs mentions datée dans un journal, un article ou essai clinique. 

Une fois les données étudiées et le besoin compris, le premier choix à faire dans le traitement des données est celui
de l'outil qui s'adapte le mieux aux données pour répondre à la problématique. La plupart des entreprises vont imposer un 
ensemble d'outils réduit selon leur choix internes. Dans ce cas, nous avons des données de taille réduite et un choix 
totalement libre. Comme les données sont structurées et que le besoin impose des croisement, j'ai choisi d'utiliser l'outil
_pandas_ qui permet de créer des dataframes et de faire des jointures facilement. Pour des plus gros volumes, il vaut mieux
utiliser spark ou des solutions clouds comme Google Big Query par exemple.  

Comme le choix de l'orchestrateur est libre, je vais choisir airflow pour sa compatibilité naturelle sur les principaux clouds. 
En cas de passage au cloud, il y aura moins de travail à fournir pour la portabilité.

### Architecture du code 
Toute la partie airflow est dans un folder appelé "dag" qui contient tous les dags. 

La partie python est dans un folder appelé src. En général, ce code est versionné et packagé puis utilisé en tant que
module lors de l'execution du dag. 
J'ai rassemblé toutes les fonctions utilitaires et susceptibles d'être utilisées dans d'autres projets dans le fichier utils
et les fonctions qui ont un but fonctionnel dans ce projet dans le fichier main. On pourrait refaire des subdivisions selon 
la fonctionnalité (preprocess/ process) au besoin. 

Les données sont dans 3 folders différents car je suppose qu'ils viennent par des pipelines différentes et seront stockées 
dans le datalake ou le cloud à différents endroits pour des question de droits et d'organisation. 

Enfin la partie TEST va rassembler les test unitaires pour les fonctions utils et les tests fonctionnels pour les fonctions 
fonctionnelles présentes dans main. 

### Présentation de la solution 

Conformément à nos observations précédentes sur les données, j'ai choisi de créer 2 dags distincts. 
Le premier dag va se charger du prépocessing. Il s'agit d'une opération courante lorsque l'on traite des données brutes 
et ce dag peut être enrichi. Selon le type de données ou d'arrivé, il peut être fait en batch ou en streaming etc. 
En général, si les données raw arrivent de manière fréquente, les tables silver sont partitionnées par date ce qui permet 
de ne traiter qu'une partie des données à chaque fois et reduire les coûts pour les traitements. 

Par simplicité et manque de temps, je n'ai crée qu'une seule fonction qui fait un nettoyage très léger, comme dit précédemment, 
juste l'harmonisation de la casse mais chaque source peut avoir son propre dag avec ses propres traitements etc ... 
Puisque qu'on se base par la suite sur les données silver, il y a aussi un découplage fonctionnel entre la source et 
le traitement des données. Un changement dans la source n'aura pas d'impact sur les projets qui l'utilise si le prétraitment
arrive à le corriger. 

Ensuite nous avons le dag de traitement. Dans ce dag, nous effectuons les étapes suivantes : 
    - chargement des sources 
    - jointures avec la table drugs 
    - harmonisation des tables selon le graphe exprimé en besoin 
    - union des tables et ecriture en sortie dans 1 seul fichier. 

Pour la sortie, je pense que la modélisation doit répondre à des contraintes de facilité d'utilisation pour les
utilisateurs. Par ailleurs, comme je travaille en pandas et que ce n'est pas la meilleur solution pour gérer des tables
nestées j'ai choisi de garder une modélisation plate et dénormalisée pour faciliter les traitements de filtre. 

J'utiliserais donc la modélisation suivante : [{drug,date,type,value}]. La colonne type permet de distinguer le type des
valeurs parmi 'journal', 'title' ou 'scientific_title'.

### Annexe : traitement Ad-hoc

Ce traitement demande d'extraire le journal qui publie le plus de médicaments. 
J'ai décidé d'utiliser un double group by mais il est aussi possible d'utiliser des window functions. 
L'optimisation dépend surtout de la manière dont sont stockées les données (types de partitions, clustering) etc. 

## Pour aller plus loin 

Quels sont les éléments à faire évoluer pour qu'il gère de grosses volumétries ? 

Le traitement de grosses volumétries va surtout demander un changement d'outil de calcul. 
La base airflow est presque indépendante de l'outil utilisé, il faut seulement changer d'opérateur pour s'adapter à
l'outil utilisé (ex: bigquery operator pour l'outil bigquery à la place de pythonoperator).

Le traitement en lui même va demander de s'adatper au nouvel outil. 
Les grosses volumétries vont souvent utiliser un opérateur cloud. 
Dans mon cas, j'ai plus utilisé GCP et il y a plusieurs solutions. 
On peut soit utiliser directment Big query très capable pour faire des requetes sur des To de données, mais il est aussi
possible d'utiliser flink ou spark soit au travers d'un cluster (dataproc, GKE, etc.) qui va faire tourner le code. 
Dans tous les cas il faudra adatper le code écrit ici en pandas au nouvel outil plus performant. 

Dans le cas de spark, il faudrait remplacer des .loc par des filters et remodifier la jointure pour utiliser par exemple 
df_join = df_pubmed.join(df_drugs, df_pubmed.title.contains(df_drugs.drug), how='left')

Côté architecture, il faudrait ajouter une platerforme ci/cd qui permet de faire tourner les tests unitaires de maniere
automatique, en cas de succes de versionner puis d'envoyer le package et/ou les dags au bon endroit pour utilisation (ex. composer) et 
créer la doc automatiquement (avec sphynx par exemple).

De plus, la partie Docker + airflow pourrait être améliorée aussi pour intégrer d'autres fonctionnalitées. 

### Requetes SQL 

Dans cette première requete, il faut prendre en compte que le cumul des ventes représente le nombre de produits vendus 
multiplié par le prix de chaque produit. 
Heureusement, on peut créer un colonne comme produit de 2 colonnes qui contiennent des nombres. 
Ensuite on fait juste un group by par date pour calculer la somme par jour. 

Ici, on remarque un piège de l'énoncé qui nous demande de filter les dates sur l'année 2019 mais nous 
montre un exemple avec des données de 2020 

    SELECT date, sum(prod_price * prod_qty) AS ventes
    FROM TRANSACTION 
    WHERE DATE(date) > DATE("01/01/2019") and DATE(date) < DATE("31/12/2019")
    GROUP BY date 
    ORDER BY date ASC 


Dans la seconde question, il s'agit d'abord de faire une jointure entre la table des ventes et la table de reference du 
catalogue. Il faut faire attention car les colonnes n'ont pas le même nom. 

Il y a aussi le besoin de filtrer les dates sur l'année 2020, le but est bien sur de scanner uniquement les partitions 
dont on a besoin et de reduire drastiquement le cout de la jointure. 
Selon l'outil utilisé (ex. GBQ), mettre le filtre en condition
du join permet d'appliquer le filtre sur la table AVANT la jointure. Comme ce n'est pas imposé par le protocole SQL, 
nous allons utiliser une sous requete qu'on viendra appeler avec le mot clef WITH. 


    SELECT T.date, T.client_id, T.prod_sales, P.product_type 
    FROM (
        SELECT 
        date, client_id, prod_id, (prod_price * prod_qty) AS ventes
        FROM TRANSACTION
        WHERE DATE(date) > DATE("01/01/2020") and DATE(date) < DATE("31/12/2020")
        )
    AS T 
    LEFT JOIN PRODCUT_NOMENCLATURE P 
    on T.prod_id = P.product_id

Pour la suite, l'énoncé demande de changer l'axe des ventes sur plusieurs colonnes. Cette opération se fait facilement 
avec l'opérateur PIVOT sur big query par exemple mais n'est pas un standard sql 

    
    WITH SUB_REQUEST_JOIN as (
        SELECT T.client_id, T.prod_sales, P.product_type 
        FROM (
            SELECT 
            date, client_id, prod_id, (prod_price * prod_qty) AS ventes
            FROM TRANSACTION
            WHERE DATE(date) > DATE("01/01/2020") and DATE(date) < DATE("31/12/2020")
            )
        as T 
        left join PRODCUT_NOMENCLATURE P 
        on T.prod_id = P.product_id
    )
    SELECT * FROM
      SUB_REQUEST_JOIN
      PIVOT(SUM(sales) FOR product_type IN ('MEUBLE', 'DECO'))

Sinon il y a plusieurs autres solutions. 
La plus simple est de séparer la nomenclature en 2 tables et réaliser un "inner join" pour filtrer la table des ventes
puis refaire une jointure entre les 2 tables aggrégées pour retrouver la table finale
Elle a le défaut d'être plutot longue à lire et répétitive... 

        
    SELECT COALESCE(MEUBLE.client_id, DECO.client_id), MEUBLE.ventes_meubles, DECO.ventes_deco
    FROM
        (
        SELECT client_id, sum(ventes) as ventes_meubles 
        FROM (
            SELECT T.client_id, T.ventes
            FROM (
                SELECT 
                date, client_id, prod_id, (prod_price * prod_qty) AS ventes
                FROM TRANSACTION
                WHERE DATE(date) > DATE("01/01/2020") and DATE(date) < DATE("31/12/2020")
            )
             as T 
            INNER JOIN (
                SELECT produt_id
                FROM PRODCUT_NOMENCLATURE
                where product_type = 'MEUBLE') P 
            ON T.prod_id = P.product_id
        GROUP BY client_id ) AS MEUBLE
    FULL JOIN
        (SELECT client_id, sum(ventes) as ventes_deco
        FROM (
            SELECT T.client_id, T.ventes
            FROM (
                SELECT 
                date, client_id, prod_id, (prod_price * prod_qty) AS ventes
                FROM TRANSACTION
                WHERE DATE(date) > DATE("01/01/2020") and DATE(date) < DATE("31/12/2020")
            )
             as T 
            INNER JOIN (
                SELECT produt_id
                FROM PRODCUT_NOMENCLATURE
                where product_type = 'DECO') P 
            ON T.prod_id = P.product_id
        GROUP BY client_id ) AS DECO
    ON DECO.client_id = MEUBLE.client_id
