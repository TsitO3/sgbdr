import json
import os


class DBCore:
    def __init__(self):
        self.DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
        self.STRUCTURE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'structure')
        self.CURRENT_DB  = None
        self.schemas = {}
        self.DATABASES = []
        
        
    def load_db(self):

        databases = []
        try:
            all_entries = os.listdir(self.STRUCTURE_DIR)
            for entry in all_entries:
                full_path = os.path.join(self.STRUCTURE_DIR, entry)
                if os.path.isdir(full_path):
                    if not entry.startswith('.'):
                        databases.append(entry)

        except Exception as e:
            print(f"❌ Erreur lors de la liste des bases de données: {e}")  

        self.DATABASES = databases


        if not self.CURRENT_DB:
            print("Aucune base de données sélectionnée. Schémas non chargés.")
            return
        
        self.schemas = {}
        try:
            schema_list =  os.listdir(os.path.join(self.STRUCTURE_DIR,self.CURRENT_DB))
            if schema_list:
                for filename in schema_list:
                    table_name = filename.replace("_schema.json", "")
                    schema_path = os.path.join(self.STRUCTURE_DIR,self.CURRENT_DB, filename)
                    
                    with open(schema_path, 'r', encoding='utf-8') as f:
                        schema = json.load(f)
         
                    self.schemas[table_name] = schema

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
        if not self.CURRENT_DB:
            raise Exception("❌ Erreur: Aucune base de données sélectionnée pour lire les données.")
        elif not table_name:
            raise ValueError("Le nom de la table ne peut pas être vide.")
        elif table_name not in self.schemas:
            raise Exception(f"❌ Erreur: La table {table_name} inconnue. Faites 'SHOW TABLES' pour lister les tables")

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
        if not self.CURRENT_DB:
            raise Exception("❌ Erreur: Aucune base de données sélectionnée pour écrire les données.")

        try:
            data_path = self._get_data_path(table_name, self.CURRENT_DB)
            
            with open(data_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
                
            
        except FileNotFoundError:
            print(f"❌ Erreur critique: Impossible de trouver ou de créer le chemin pour écrire les données : {data_path}")
            raise
        except Exception as e:
            print(f"❌ Erreur lors de l'écriture des données de la table '{table_name}' : {e}")
            raise

    
    def create_database(self, database_name: str):
        if not database_name:
            raise ValueError("Le nom de la base de données ne peut pas être vide.")
       
        db_data_path = os.path.join(self.DATA_DIR, database_name)
        db_struct_path = os.path.join(self.STRUCTURE_DIR, database_name)
        
        
        if database_name in self.DATABASES:
            raise ValueError(f"❌ Erreur: La base de données '{database_name}' existe déjà.")

        try:
            os.makedirs(db_data_path)
            os.makedirs(db_struct_path)
            
            print(f"✅ Base de données '{database_name}' créée avec succès.")
            self.load_db()

        except Exception as e:
            print(f"❌ Erreur lors de la création des dossiers de la base de données : {e}")
            if os.path.exists(db_data_path): os.rmdir(db_data_path)
            if os.path.exists(db_struct_path): os.rmdir(db_struct_path)
            raise
        
    def show_tables(self):
        if not self.CURRENT_DB:
            print("❌ Erreur: Aucune base de données sélectionnée. Utilisez la commande 'USE <db_name>'.")
            return
        
        if not self.schemas:
            print(f"\nTables dans la base de données '{self.CURRENT_DB}' :")
            print("=========================")
            print("| Aucune table trouvée.      |")
            print("=========================")
            return
            
            
        max_len = max(len(t) for t in self.schemas)
        col_width = max_len + 4 
        separator_width = col_width + 3 
        separator_line = "=" * separator_width

        print(f"\nTables dans la base de données '{self.CURRENT_DB}' :")
        print(separator_line)
        
        for table in sorted(self.schemas):
            print(f"| {table.ljust(col_width)}|")

        print(separator_line)

        

    def show_databases(self):
        print("\nBases de données disponibles :")
           
        if not self.DATABASES:
            print("==============================")
            print("| Aucune base de données trouvée. |")
            print("==============================")
            return
        
        max_len = max(len(d) for d in self.DATABASES)
        col_width = max_len + 4 
        separator_width = col_width + 3 
        separator_line = "=" * separator_width

        print(separator_line)
        
        for db_name in sorted(self.DATABASES):
            print(f"| {db_name.ljust(col_width)}|")

        print(separator_line)
          

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
        if not self.CURRENT_DB:
            raise Exception("❌ Erreur: Aucune base de données sélectionnée. Utilisez 'USE <db_name>' avant de créer une table.")

        if not table_name:
            raise ValueError("Le nom de la table ne peut pas être vide.")
        
        if table_name in self.schemas.keys():
            raise ValueError(f"❌ Erreur: La table '{table_name}' existe déjà dans la base de données '{self.CURRENT_DB}'.")
            
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
            
            default_value = None
            for part in parts:
                if part.startswith('default='):
                    default_value = part.split('=', 1)[1].strip('"').strip("'") 
                    break

            if default_value is not None:
                if self._validate_type(default_value, column_type):
                    field_definition["default"] = default_value
                else:
                    determined_type = type(default_value).__name__
                    raise TypeError(
                        f"La valeur par défaut '{default_value}' (Type Python: {determined_type}) "
                        f"n'est pas valide pour le type de colonne déclaré: '{column_type}'."
                    )

            if is_required:
                if default_value and self._validate_type(default_value, column_type):
                    field_definition["required"] = True
                else:
                    raise TypeError(
                        f"Les conditions 'notnull' doit avoir un valeur par defaut"
                    )
            elif is_pk:
                field_definition["required"] = True
                
                
            

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
                
            print(f"✅ Table '{table_name}' créée avec succès dans la base de données '{self.CURRENT_DB}'.")

            self.load_db()
            
        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde de la table : {e}")
            raise

    def describe_table(self, table_name):
        if not self.CURRENT_DB:
            raise Exception("❌ Erreur: Aucune base de données sélectionnée pour lire les données.")
        elif not table_name:
            raise ValueError("Le nom de la table ne peut pas être vide.")
        elif table_name not in self.schemas:
            raise Exception(f"❌ Erreur: La table {table_name} inconnue. Faites 'SHOW TABLES' pour lister les tables")
        else:
            schema = self.schemas[table_name]
            schema = self.schemas[table_name]
            fields = schema.get('fields', [])
            
            max_col_len = max(len(f['column']) for f in fields) if fields else 10
            
            COL_NAME_WIDTH = max_col_len + 2
            TYPE_WIDTH = 10
            NULL_WIDTH = 6
            KEY_WIDTH = 6
            DEFAULT_WIDTH = 15
            
            separator_width = COL_NAME_WIDTH + TYPE_WIDTH + NULL_WIDTH + KEY_WIDTH + DEFAULT_WIDTH

            separator_line = "=" * (separator_width+16)

            print(f"\nDescription de la table '{table_name}' dans '{self.CURRENT_DB}' :")
            print(separator_line)

            header = f"| {'Field'.ljust(COL_NAME_WIDTH)} | {'Type'.ljust(TYPE_WIDTH)} | {'Null'.ljust(NULL_WIDTH)} | {'Key'.ljust(KEY_WIDTH)} | {'Default'.ljust(DEFAULT_WIDTH)} |"
            print(header)
            print(separator_line)

            for field in fields:
                column = field['column']
                dtype = field['type']
                is_pk = 'PK' if field.get('primary_key') else ''
                allows_null = 'NO' if field.get('required') else 'YES'
                default_val = field.get('default')
                if default_val is not None:
                    display_default = str(default_val).replace('\n', ' ').ljust(DEFAULT_WIDTH)
                else:
                    display_default = 'NULL'.ljust(DEFAULT_WIDTH)
                key_display = is_pk
                if field.get('auto_increment'):
                    key_display = 'PK A/I' if is_pk else 'A/I'
                
                row = (
                    f"| {column.ljust(COL_NAME_WIDTH)} "
                    f"| {dtype.ljust(TYPE_WIDTH)} "
                    f"| {allows_null.ljust(NULL_WIDTH)} "
                    f"| {key_display.ljust(KEY_WIDTH)} "
                    f"| {display_default.ljust(DEFAULT_WIDTH)} |"
                )
                print(row)

            print(separator_line)

    def insert_data(self, table_name: str, values: list):
        if not self.CURRENT_DB:
            raise Exception("❌ Erreur: Aucune base de données sélectionnée (USE DB).")

        if table_name not in self.schemas:
            raise ValueError(f"❌ Erreur: La table '{table_name}' n'existe pas ou n'est pas chargée.")
            
        schema = self.schemas[table_name]
        records = self._read_data(table_name)
        new_record = {}
        
        value_index = 0
        
        for field in schema['fields']:
            column_name = field['column']
            expected_type = field['type']
            
            if field.get('auto_increment'):
                new_id = self.auto_increment_counters.get(table_name, 1)
                new_record[column_name] = new_id
                # 2. Incrémente le compteur pour le prochain enregistrement
                self.auto_increment_counters[table_name] = new_id + 1
                # L'ID est généré, on passe au champ suivant du schéma
                continue 

            # --- Détermination de la Valeur ---
            
            # Vérifier s'il reste des valeurs fournies par l'utilisateur à traiter
            if value_index < len(values):
                value = values[value_index]
                value_index += 1 # Préparer l'index pour la prochaine valeur utilisateur
                source = "user"
            else:
                value = None # Aucune valeur fournie par l'utilisateur
                source = "none"

            # --- Application des Contraintes NOT NULL et DEFAULT ---
            
            is_required = field.get('required', False)
            default_value = field.get('default')

            # Si la valeur est absente de l'input utilisateur (None ou index dépassé)
            if value is None and source == "none":
                
                if default_value is not None:
                    # Utiliser la valeur par défaut si elle existe
                    value = default_value
                elif is_required:
                    # Rejeter l'insertion si le champ est requis et qu'il n'y a pas de valeur par défaut
                    raise ValueError(f"❌ Erreur: La colonne requise '{column_name}' est manquante et n'a pas de valeur par défaut.")
                else:
                    # Le champ n'est pas requis et n'a pas de valeur (sera stocké comme None/null)
                    new_record[column_name] = None
                    continue # Passer au champ suivant du schéma

            # --- Validation Finale de la Valeur (utilisez _validate_type) ---

            if not self._validate_type(value, expected_type):
                # Erreur de type : peut arriver même avec default_value si l'utilisateur a donné un input
                # ou si default_value est d'un type incorrect (bien que vérifié dans create_table)
                determined_type = type(value).__name__
                raise TypeError(
                    f"❌ Erreur de type pour la colonne '{column_name}'. Valeur '{value}' (Type: {determined_type}) "
                    f"n'est pas valide pour le type attendu: '{expected_type}'."
                )

            # --- Ajout de la valeur validée ---
            new_record[column_name] = value

        # --- 2. Validation des Contraintes d'Unicité (PK non auto-incrémentée) ---
        
        for field in schema['fields']:
            if field.get('primary_key') and not field.get('auto_increment'):
                pk_column = field['column']
                pk_value = new_record[pk_column]
                
                # Vérifier si cette valeur PK existe déjà
                if any(record.get(pk_column) == pk_value for record in records):
                    raise ValueError(f"❌ Erreur de contrainte: La valeur '{pk_value}' est déjà utilisée pour la clé primaire '{pk_column}'.")
        
        # --- 3. Insertion et Sauvegarde ---
        records.append(new_record)
        self._write_data(table_name, records)
        
        # Message de confirmation
        generated_id = new_record.get(pk_column, new_record.get(schema['fields'][0]['column'])) 
        print(f"✅ Insertion réussie dans '{table_name}'. Enregistrement ajouté.")

        return generated_id
        
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