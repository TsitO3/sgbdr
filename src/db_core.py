import json
import os

DATA_DIR = 'data'
STRUCTURE_DIR = 'structure'

class DBCore:
    import os

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
            return []
            
        except json.JSONDecodeError:
            print(f"❌ Erreur: Le fichier de données de la table '{table_name}' est corrompu (JSON invalide).")
            raise
        
    def _write_data(self, table_name: str, data: list):
        if not self.current_db:
            raise Exception("❌ Erreur: Aucune base de données sélectionnée pour écrire les données.")

        try:
            data_path = self._get_data_path(table_name, self.current_db)
            
            with open(data_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
                
            
        except FileNotFoundError:
            print(f"❌ Erreur critique: Impossible de trouver ou de créer le chemin pour écrire les données : {data_path}")
            raise
        except Exception as e:
            print(f"❌ Erreur lors de l'écriture des données de la table '{table_name}' : {e}")
            raise
        
    def show_tables(self):
        if not self.current_db:
            print("❌ Erreur: Aucune base de données sélectionnée. Utilisez la commande 'USE <db_name>'.")
            return

        db_struct_dir = os.path.join(self.STRUCTURE_DIR, self.current_db)
        
        try:
            filenames = os.listdir(db_struct_dir)
            tables = []
            for filename in filenames:
                if filename.endswith("_schema.json"):
                    table_name = filename.replace("_schema.json", "")
                    tables.append(table_name)

            if not tables:
                print(f"\nTables dans la base de données '{self.current_db}' :")
                print("=========================")
                print("| Aucune table trouvée.      |")
                print("=========================")
                return
            
            
            max_len = max(len(t) for t in tables)
            col_width = max_len + 4 
            separator_width = col_width + 3 
            separator_line = "=" * separator_width

            print(f"\nTables dans la base de données '{self.current_db}' :")
            print(separator_line)
            
            for table in sorted(tables):
                print(f"| {table.ljust(col_width)}|")

            print(separator_line)

        except FileNotFoundError:
            print(f"\nTables dans la base de données '{self.current_db}' :")
            print("=========================")
            print(f"| Le dossier de structure pour '{self.current_db}' est introuvable. |")
            print("=========================")
        except Exception as e:
            print(f"\nErreur inattendue lors de la liste des tables: {e}")

    def show_databases(self):
        print("\nBases de données disponibles :")
        
        try:
            all_entries = os.listdir(self.STRUCTURE_DIR)
            databases = []
            for entry in all_entries:
                full_path = os.path.join(self.STRUCTURE_DIR, entry)
                if os.path.isdir(full_path):
                    if not entry.startswith('.'):
                        databases.append(entry)
            
            if not databases:
                print("==============================")
                print("| Aucune base de données trouvée. |")
                print("==============================")
                return
            
            max_len = max(len(d) for d in databases)
            col_width = max_len + 4 
            separator_width = col_width + 3 
            separator_line = "=" * separator_width

            print(separator_line)
            
            for db_name in sorted(databases):
                print(f"| {db_name.ljust(col_width)}|")

            print(separator_line)

        except Exception as e:
            print(f"❌ Erreur lors de la liste des bases de données: {e}")    

    def _validate_type(self, value, expected_type: str) -> bool:
        expected_type = expected_type.lower()

        if expected_type == 'string':
            return value is not None
        
        if expected_type == 'integer':
            if isinstance(value, int):
                return True
            if isinstance(value, str):
                try:
                    int(value) 
                    return True
                except ValueError:
                    return False
            return False

        if expected_type == 'float':
            if isinstance(value, (int, float)):
                return True
            if isinstance(value, str):
                try:
                    float(value)
                    return True
                except ValueError:
                    return False
            return False
            
        if expected_type == 'boolean':
            if isinstance(value, bool):
                return True
            if isinstance(value, str):
                return value.lower() in ('true', 'false', '1', '0')
            return False

        return False

    def create_table(self, table_name: str, fields_def: list):
        if not self.current_db:
            raise Exception("❌ Erreur: Aucune base de données sélectionnée. Utilisez 'USE <db_name>' avant de créer une table.")

        if not table_name:
            raise ValueError("Le nom de la table ne peut pas être vide.")
        
        if table_name in self.schemas.keys():
            raise ValueError(f"❌ Erreur: La table '{table_name}' existe déjà dans la base de données '{self.current_db}'.")
            
        VALID_TYPES = ["integer", "string", "float", "boolean"]
        
        new_schema = {
            "name": table_name,
            "fields": []
        }
        
        pk_count = 0 
        
        for field_str in fields_def:
            # Format attendu : nom:type[:PK][:AUTO]
            parts = field_str.lower().split(':')
            
            if len(parts) < 2:
                raise ValueError(f"Définition de champ invalide : {field_str}. Format attendu: nom:type.")
            
            column_name = parts[0]
            column_type = parts[1]
            
            if column_type not in VALID_TYPES:
                raise ValueError(f"Type de donnée non valide '{column_type}' pour la colonne '{column_name}'. Types valides: {', '.join(VALID_TYPES)}")

            field_definition = {
                "column": column_name,
                "type": column_type
            }

            is_pk = 'pk' in parts or 'primary_key' in parts
            is_auto = 'auto' in parts or 'auto_increment' in parts
            
            if is_pk:
                pk_count += 1
                if pk_count > 1:
                    raise ValueError(f"Une seule clé primaire (PK) est autorisée par table. Problème avec '{column_name}'.")
                
                field_definition["primary_key"] = True
                field_definition["required"] = True
            
            if is_auto:
                if not is_pk:
                    raise ValueError(f"L'auto-incrément nécessite que le champ '{column_name}' soit une clé primaire (PK).")
                if column_type != 'integer':
                    raise ValueError("L'auto-incrément ne peut être appliqué qu'à un type 'integer'.")
                    
                field_definition["auto_increment"] = True


            
            is_required = 'notnull' in parts or 'required' in parts
            
            if is_required or is_pk:
                field_definition["required"] = True
            
            default_value = None
            for part in parts:
                if part.startswith('default='):
                    default_value = part.split('=', 1)[1].strip('"').strip("'") 
                    break

            if default_value is not None:
                field_definition["default"] = default_value
                
            if not self._validate_type(default_value, column_type):
                     determined_type = type(default_value).__name__
                     raise TypeError(
                         f"La valeur par défaut '{default_value}' (Type Python: {determined_type}) "
                         f"n'est pas valide pour le type de colonne déclaré: '{column_type}'."
                     )

            new_schema["fields"].append(field_definition)

        if not new_schema["fields"]:
             raise ValueError("Doit définir au moins une colonne pour la table.")

        try:
            schema_path = self._get_schema_path(table_name)
            data_path = self._get_data_path(table_name)

            with open(schema_path, 'w', encoding='utf-8') as f:
                json.dump(new_schema, f, indent=4, ensure_ascii=False)
                
            with open(data_path, 'w', encoding='utf-8') as f:
                json.dump([], f, indent=4, ensure_ascii=False)
                
            print(f"✅ Table '{table_name}' créée avec succès dans la base de données '{self.current_db}'.")

            self.load_db()
            
        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde de la table : {e}")
            raise

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