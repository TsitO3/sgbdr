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
        
        
        
        
    def load_db(self):
        print("Chargement des structures de base de données...")
        
        try:
            for filename in os.listdir(self.STRUCTURE_DIR):
                table_name = filename.replace("_schema.json", "")
                schema_path = os.path.join(self.STRUCTURE_DIR, filename)
                
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

    def _get_schema_path(self, table_name):
        # Retourne le chemin complet du fichier de structure (ex: structure/users_schema.json)
        pass

    def _get_data_path(self, table_name):
        # Retourne le chemin complet du fichier de données (ex: data/users.json)
        pass
        
    def _read_data(self, table_name):
        # Lit le fichier JSON de la table et retourne les données (liste de dicts)
        pass
        
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