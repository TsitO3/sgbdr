import json
import os

DATA_DIR = 'data'
STRUCTURE_DIR = 'structure'

class DBCore:
    import os
# ... (autres imports comme json)

class DBCore:
    def __init__(self):
        self.DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
        self.STRUCTURE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'structure')
        self.CURRENT_DB  = None
        self.schemas = {}
        
        
    def load_db(self):

        if not self.CURRENT_DB:
            print("Aucune base de données sélectionnée. Schémas non chargés.")
            return

        print("Chargement des structures de base de données...")
        
        self.schemas = {}
        try:
            for filename in os.listdir(self.STRUCTURE_DIR):
                table_name = filename.replace("_schema.json", "")
                schema_path = os.path.join(self.STRUCTURE_DIR,self.CURRENT_DB, filename)
                
                with open(schema_path, 'r', encoding='utf-8') as f:
                    schema = json.load(f)
                    
                    self.schemas[table_name] = schema
                    print(f"   - Schéma chargé pour la table '{table_name}'")
                        
            print(f"{len(self.schemas)} schéma(s) chargé(s) avec succès.")
            
        except FileNotFoundError:
            print(f"⚠️ Avertissement: Le dossier de structures ({self.STRUCTURE_DIR}) est vide ou manquant. Aucune table chargée.")
        except json.JSONDecodeError as e:
            print(f"❌ Erreur: Fichier de schéma JSON invalide : {filename}. Détails : {e}")
        except Exception as e:
            print(f"❌ Une erreur inattendue est survenue lors du chargement des schémas : {e}")

    def use_db(self, database_name: str):
        self.CURRENT_DB = database_name
        
        db_data_path = os.path.join(self.DATA_DIR, database_name)
        db_struct_path = os.path.join(self.STRUCTURE_DIR, database_name)
        
        if not os.path.exists(db_data_path) or not os.path.exists(db_struct_path):
            print(f"❌ Database {database_name} introuvable")
        else:
            self.load_db() 
            print(f"✅ Connecté à la base de données '{database_name}'.")

    def _get_schema_path(self, table_name: str) -> str:
        filename = f"{table_name}_schema.json"
        schema_path = os.path.join(self.STRUCTURE_DIR,self.CURRENT_DB, filename)
        return schema_path

    def _get_data_path(self, table_name):
        filename = f"{table_name}_data.json"
        schema_path = os.path.join(self.DATA_DIR,self.CURRENT_DB, filename)
        return schema_path
        
    def _read_data(self, table_name: str) -> list:
        if not self.current_db:
            raise Exception("❌ Erreur: Aucune base de données sélectionnée pour lire les données.")

        try:
            data_path = self._get_data_path(table_name)
            
            with open(data_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if not isinstance(data, list):
                raise TypeError("Le fichier de données JSON n'est pas une liste d'enregistrements valide.")
                
            return data
            
        except FileNotFoundError:
            # C'est normal si une table n'a pas encore de données, ou si elle n'existe pas.
            # On retourne une liste vide dans ce cas.
            return []
            
        except json.JSONDecodeError:
            # Si le fichier JSON est corrompu ou vide sans crochets.
            print(f"❌ Erreur: Le fichier de données de la table '{table_name}' est corrompu (JSON invalide).")
            # Lever l'exception pour arrêter l'opération courante
            raise
        
    def _write_data(self, table_name, data):
        # Écrit la liste de dicts dans le fichier JSON de la table
        pass
        
    def show_tables(self):
        # Liste les fichiers dans /data ou /structure pour afficher les tables
        pass

    # --- B. Commandes DDL (Définition des Données) ---
    def create_table(self, table_name, fields_def):
        # Crée un fichier de schéma et un fichier de données vide
        pass

    # --- C. Commandes DML (Manipulation des Données - CRUD) ---
    def insert_data(self, table_name, values):
        # Valide les données avec le schéma, ajoute l'enregistrement et écrit le fichier
        pass
        
    def select_all(self, table_name):
        # Lit les données et les retourne toutes
        pass
        
    def update_data(self, table_name, update_clause):
        # Modifie les enregistrements correspondant à la clause WHERE
        pass
        
    def delete_data(self, table_name, where_clause):
        # Supprime les enregistrements correspondant à la clause WHERE
        pass


db_core = DBCore()