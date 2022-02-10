# Projet GDELT

[comment]: <> (TABLE OF CONTENTS ===============================================================================================================)
##### [Introduction](#_introduction)
##### [Présentation du jeu de données](#_part1)
##### [Choix des technologies](#_part2)
##### [Configuration du cluster](#_part3)
##### [Preprocessing](#_part4)

[comment]: <> (=================================================================================================================================)

# Introduction <a name="_introduction"></a>

Ce projet a été réalisé dans le cadre du cours de bases de données non relationnelles du MS Big Data de Télécom Paris. L'objectif est proposer un système de stockage distribué, résilient et performant pour repondre aux question suivantes :
- afficher le nombre d’articles/évènements qu’il y a eu pour chaque triplet (jour, pays de l’évènement, langue de l’article).

- pour un pays donné en paramètre, affichez les évènements qui y ont eu place triées par le nombre de mentions (tri décroissant); permettez une agrégation par jour/mois/année

- pour une source de donnés passée en paramètre (gkg.SourceCommonName) affichez les thèmes, personnes, lieux dont les articles de cette sources parlent ainsi que le nombre d’articles et le ton moyen des articles (pour chaque thème/personne/lieu); permettez une agrégation par jour/mois/année.

- étudiez l’évolution des relations entre deux pays (specifies en paramètre) au cours de l’année. Vous pouvez vous baser sur la langue de l’article, le ton moyen des articles, les themes plus souvent citées, les personalités ou tout element qui vous semble pertinent.

# Présentation du jeu de données <a name="_part1"></a> 
[comment]: <> (=================================================================================================================================)
Le jeu de données est composé de trois tables. 
- La table Export contient des informations sur des articles de presse : date de l'évènement, informations sur les acteurs mentionnés, informations sur l'évènement... 
- La table Mentions met en relation les articles mentionnés dans d'autres.
- La table GKG permet de connecter chaque élément ensemble : personnes, organisations, localisation, thème... pour rendre compte de ce qui se passe dans le monde.
- it connects every person, organization, location, count, theme,


<img src="figures/tables_schema.PNG" alt="drawing" width="700"/>


# Choix des technologies <a name="_part2"></a>
[comment]: <> (=================================================================================================================================)

La technologie que nous avons retenu est Cassandra. Bien que la scalabilité ne soit pas nécessairement recherchée puisque l'on travaille sur 5 machines, l'avantage va à Cassandra pour ses multiples noeuds maîtres. Cassandra est tolérant vis-à-vis des pannes et offre une bonne disponibilité des données (architecture P2P et réplication des données). Cassandra est relativement rapide en écriture. Par ailleurs, contrairement à la tolérance aux pannes, la vitesse de lecture n’est pas une contrainte pour le projet.
Enfin, les VM de l’école disposent d’une RAM limitée, rendant MongoDB peu opérant dans l’utilisation de la mémoire pour le stockage des données de travail.
L’avantage va donc à Cassandra.

Pour réaliser ce projet, telles sont les contraintes qui nous sont imposées :
-	Peu d’espace disque disponible (maximum 200 Go en tout, données système comprises)
-	Peu de RAM sur les VM
-	Tolérance à la panne d’un nœud nécessaire
-	Accès aux données moyennement rapide (<5min ?)


<img src="figures/table_cass_vs_mongo.PNG" alt="drawing" width="800"/>

# Configuration du cluster <a name="_part3"></a>
[comment]: <> (=================================================================================================================================)

### Installation de Java 8

Mise à jour des tous les packages existants  
`sudo apt-get update -y`

Installation des packages  
`sudo apt-get install -y openjdk-8-jre-headless`

Vérification de la bonne exécution des commandes précédentes et des logs  
<img src="figures/java-1.png" alt="drawing" width="800"/>  
  
<img src="figures/java-2.png" alt="drawing" width="800"/>


### Installation de Cassandra

Téléchargement des fichiers d'installation

`wget https://dlcdn.apache.org/cassandra/4.0.1/apache-cassandra-4.0.1-bin.tar.gz`

Vérification de l’intégrité du paquet téléchargé

`gpg --print-md SHA256 apache-cassandra-4.0.1-bin.tar.gz`

Décompression et suppression de l'archive

`tar xzvf apache-cassandra-4.0.1-bin.tar.gz`  

`rm -r apache-cassandra-4.0.1-bin.tar.gz`  
 
 L'installation s'est correctement terminée :
 <img src="figures/java-3.png" alt="drawing" width="800"/>
 
### Configuration de Cassandra
Les machines utilisées pour le projet sont :  
  
tp-hadoop-15 | 192.168.3.187  
tp-hadoop-12 | 192.168.3.93  
tp-hadoop-33 | 192.168.3.46  
tp-hadoop-11 | 192.168.3.168  
tp-hadoop-19 | 192.168.3.142  

Configuration du fichier apache-cassandra-4.0.1/conf/cassandra.yaml

**Sur tp-hadoop-15 :**  
seeds : "192.168.3.187,192.168.3.93,192.168.3.46,192.168.3.168,192.168.3.142"  
listen_address : 192.168.3.187  
rpc_address : 192.168.3.187  

**Sur tp-hadoop-12 :**  
seeds : "192.168.3.187,192.168.3.93,192.168.3.46,192.168.3.168,192.168.3.142"  
listen_address : 192.168.3.93  
rpc_address : 192.168.3.93  

**Sur tp-hadoop-33 :**  
seeds : "192.168.3.187,192.168.3.93,192.168.3.46,192.168.3.168,192.168.3.142"  
listen_address : 192.168.3.46  
rpc_address : 192.168.3.46  

**Sur tp-hadoop-11 :**  
seeds : "192.168.3.187,192.168.3.93,192.168.3.46,192.168.3.168,192.168.3.142"  
listen_address : 192.168.3.168  
rpc_address : 192.168.3.168  

**Sur tp-hadoop-19 :**  
seeds : "192.168.3.187,192.168.3.93,192.168.3.46,192.168.3.168,192.168.3.142"  
listen_address : 192.168.3.142  
rpc_address : 192.168.3.142  

Configuration de 


# Preprocessing <a name="_part4"></a>
•	Scrapping des URL : 
    •	masterfilelist.txt
    •	masterfilelist-translation.txt
    
•	Stockage des URL de téléchargement “.zip” dans deux dataframes : 
    •	Un dataframe pour les URL scrappées masterfilelist.txt
    •	Un dataframe pour les URL scrappées masterfilelist-translation.txt
    
•	Ajout d’une colonne date aux dataframes en parsant la date contenu dans le lien de chaque URL “.zip”

•	Ajout d’une colonne type aux dataframes en parsant le type du fichier contenu dans le lien de chaque URL “.zip” (ex: type_csv {export, mentions, gkg, export_translation, mentions translation, gkg_translation})

•	Sélection des URL en spécifiant une date de début et une date de fin

•	Les dataframe contiennent donc un triplet d’URL pour chaque YYYY:MM-DD:H:M:S (URL export.zip, URL mentions.zip), URL gkg.zip

•	Vérification de l’URL “.zip” avec la librairie python “validators”

  •	Si une URL “.zip” n’est pas valide alors on supprime le triplet d’URL complet
  
•	Fusion des 2 dataframes d’URL “.zip”

•	Lecture des fichiers “.zip” via la librairie python “BytesIO”

•	Ouverture des fichiers “.zip” avec la librairie ZipFile. On obtient un fichier “.csv”

•	Lecture pour chaque type de csv du fichier en question et on le transforme en DataFrame

•	Concaténation des dataframes du même type
  •	Obtention des 6 dataframes suivants :
      •	dataframe export (regroupe tous les fichiers “.csv” de type export qui ont été scrappé pour   une date de début et de fin
      •	dataframe mentions (regroupe tous les fichiers “.csv” de type mentions qui ont été scrappé pour une date de début et de fin
      •	dataframe gkg (regroupe tous les fichiers “.csv” de type mentions qui ont été scrappé pour une date de début et de fin
      •	Idem pour les fichiers “.csv” de type translation
      
•	Renommage des colonnes en suivant la convention de nommage indiqué dans la document de GDELT Project

•	Transformation des dates en type datetime

•	Jointure entre des tables mentions_translation et mentions et export

•	Sélection des colonnes en question pour la préparation des ".csv" contenant les informations utiles à chaque requête
  • Parsing des colonnes récupérer les informations recherchés
