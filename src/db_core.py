import json
import os
import shutil


class DBCore:
    def __init__(self):
        self.DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
        self.STRUCTURE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'structure')
        self.CURRENT_DB  = None
        self.schemas = {}
        self.DATABASES = []
        self.DB_SYSTEM = "system"
        self.schemas_system = {}
        try:
            schema_list =  os.listdir(os.path.join(self.STRUCTURE_DIR,self.DB_SYSTEM))
            if schema_list:
                for filename in schema_list:
                    table_name = filename.replace("_schema.json", "")
                    schema_path = os.path.join(self.STRUCTURE_DIR,self.DB_SYSTEM, filename)
                    
                    with open(schema_path, 'r', encoding='utf-8') as f:
                        schema = json.load(f)
         
                    self.schemas_system[table_name] = schema
            
        except FileNotFoundError:
            print(f"⚠️ Avertissement: Le dossier de structures ({self.DB_SYSTEM}) est vide ou manquant. Aucune table chargée.")
        except json.JSONDecodeError as e:
            print(f"❌ Erreur: Fichier de schéma JSON invalide : {filename}. Détails : {e}")
        except Exception as e:
            print(f"❌ Une erreur inattendue est survenue lors du chargement des schémas : {e}")

        
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
            data_path = self._get_data_path(table_name)
            
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
            print("==============================")
            print("| Aucune table trouvée.      |")
            print("==============================")
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
            print("===================================")
            print("| Aucune base de données trouvée. |")
            print("===================================")
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


                tmp_schemas = self.schemas
                tmp_db = self.CURRENT_DB

                self.CURRENT_DB = self.DB_SYSTEM
                self.schemas = self.schemas_system

                self.insert_data("serials", [f":{table_name}:2"], True)

                self.CURRENT_DB = tmp_db
                self.schemas = tmp_schemas

            
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
    
    def get_serial_id(self, table_name: str):
        data_path = os.path.join(self.DATA_DIR, self.DB_SYSTEM, "serials_data.json")
        try:
            with open(data_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if not isinstance(data, list):
                raise TypeError("Le fichier de données JSON n'est pas une liste d'enregistrements valide.")

            
            new_data = []
            for i,elem in enumerate(data):
                if elem["nomtable"] == table_name:
                    new_id = int(elem["value"])
                    elem["value"] = str(new_id+1)
                    index = i
                    data[i] = elem
                new_data.append(data[i])

            
        except FileNotFoundError:
            print("Fichier introuvable.")
            raise
            
        except json.JSONDecodeError:
            print(f"❌ Erreur: Le fichier de données de la table '{table_name}' est corrompu (JSON invalide).")
            raise

        tmp_db = self.CURRENT_DB
        self.CURRENT_DB = self.DB_SYSTEM
        self._write_data("serials", new_data)
        self.CURRENT_DB = tmp_db
        
        return new_id

    def insert_data(self, table_name: str, listvalues: list, flag: bool = False):
        if not self.CURRENT_DB:
            raise Exception("❌ Erreur: Aucune base de données sélectionnée (USE DB).")

        if table_name not in self.schemas:
            raise ValueError(f"❌ Erreur: La table '{table_name}' n'existe pas ou n'est pas chargée.")
            
        schema = self.schemas[table_name]
        records = self._read_data(table_name)

        counter = 0
        for values in listvalues:
            new_record = {}
            counter += 1
            values = values.split(":")

            if len(values) > len(schema['fields']):
                raise ValueError(f"❌ Erreur: Trop de valeurs fournies. Attendu: {len(schema['fields'])} champs.")

            for i, field in enumerate(schema['fields']):
                column_name = field['column']
                expected_type = field['type']
                default_value = field.get('default')
                pk_constraint = field.get('primary_key')
                
                if pk_constraint:
                    if field.get('auto_increment'):
                        new_id = self.get_serial_id(table_name)
                    else:
                        if values[i] == "":
                            raise ValueError("Le clé primaire ne peut pas être null")
                        if not self._validate_type(values[i], expected_type):
                            raise ValueError(f"Le type de la clé primaire doit être {expected_type}")
                        new_id = values[i]
                    for record in records:
                        if record[column_name] == new_id:
                            raise ValueError(f"❌ Erreur de contrainte: La valeur '{new_id}' est déjà utilisée pour la clé primaire.")
                    new_record[column_name] = new_id

                else:

                    if values[i] != "":
                        value = values[i]
                        if not self._validate_type(value, expected_type):
                            determined_type = type(value).__name__
                            raise TypeError(
                                f"❌ Erreur de type pour la colonne '{column_name}'. Valeur '{value}' (Type: {determined_type}) "
                                f"n'est pas valide pour le type attendu: '{expected_type}'."
                            )

                        new_record[column_name] = value
                    else:
                        if default_value:
                            value = default_value
                        else:
                            value = None
                        new_record[column_name] = value

            
            records = self._read_data(table_name)
            records.append(new_record)
        self._write_data(table_name, records)
        
        if not flag:
            print(f"✅ Insertion réussie dans '{table_name}'. {counter} Enregistrement ajouté.")
        

    def drop_table(self, table_name: str):
        if not self.CURRENT_DB:
            raise Exception("❌ Erreur: Aucune base de données sélectionnée (USE DB).")

        if table_name not in self.schemas:
            raise ValueError(f"❌ Erreur: La table '{table_name}' n'existe pas dans la base de données '{self.CURRENT_DB}'.")

        print(f"⚠️ AVERTISSEMENT: Toutes les données de la table '{table_name}' seront DÉFINITIVEMENT supprimées.")
        confirmation = input("Êtes-vous sûr de vouloir supprimer cette table et ses enregistrements ? (oui/non) : ").lower()

        if confirmation != 'oui':
            print("Opération annulée par l'utilisateur.")
            return

        schema_path = self._get_schema_path(table_name)
        data_path = self._get_data_path(table_name)

        try:
            if os.path.exists(data_path):
                os.remove(data_path)
            
            if os.path.exists(schema_path):
                os.remove(schema_path)

            """Mettre les actions pour les 
            metadonnées plust tard"""

            self.load_db()

            print(f"✅ Table '{table_name}' supprimée avec succès.")

        except Exception as e:
            print(f"❌ Erreur lors de la suppression de la table '{table_name}': {e}")
            raise


    def drop_database(self, database_name: str):

        if database_name not in self.DATABASES:
            raise ValueError(f"❌ Erreur: La table '{database_name}' n'existe pas dans la base de données '{self.CURRENT_DB}'.")

        schema_list =  os.listdir(os.path.join(self.STRUCTURE_DIR,database_name))
                
        if schema_list:
            print(f"⚠️ AVERTISSEMENT: Toutes les données de la base '{database_name}' seront DÉFINITIVEMENT supprimées.")
            confirmation = input("Êtes-vous sûr de vouloir supprimer cette base et ses enregistrements ? (oui/non) : ").lower()

            if confirmation != 'oui':
                print("Opération annulée par l'utilisateur.")
                return

        schema_path = os.path.join(self.STRUCTURE_DIR, database_name)
        data_path = os.path.join(self.DATA_DIR, database_name)

        try:
            print(data_path)
            print(schema_path)
            if os.path.exists(data_path):
                shutil.rmtree(data_path)
            
            if os.path.exists(schema_path):
                shutil.rmtree(schema_path)

            """Mettre les actions pour les 
            metadonnées plust tard"""

            print(f"✅ Database '{database_name}' supprimée avec succès.")
            
            if database_name == self.CURRENT_DB:
                self.CURRENT_DB = None
            self.load_db()


        except Exception as e:
            print(f"❌ Erreur lors de la suppression de la database '{database_name}': {e}")
            raise


    
    def display_result(self, colname: list , resultat: list):
        col_widths = {name: len(name) for name in colname}

        for record in resultat:
            for name in colname:
                value = record.get(name)
                display_value = 'NULL' if value is None else str(value)
                col_widths[name] = max(col_widths[name], len(display_value))

        total_width = sum(col_widths.values()) + (len(colname) * 3) + 1
        separator = "=" * total_width

        print(separator)

        header_row = "|"
        for name in colname:
            width = col_widths[name]
            header_row += f" {name.center(width)} |"
        print(header_row)
        
        print(separator)

        for record in resultat:
            data_row = "|"
            for name in colname:
                width = col_widths[name]
                value = record.get(name)
                display_value = 'NULL' if value is None else str(value)
                
                data_row += f" {display_value.ljust(width)} |"
            data_row = data_row.replace('\n', ' ')
            print(data_row)

        print(separator)
    
    def _evaluate_condition(self, record: dict, parts: list) -> bool:

        if len(parts) != 3:
            raise ValueError(f"Format de condition invalide: '{' '.join(parts)}'. Format attendu: 'colonne opérateur valeur'.")

        col_name, operator, raw_target_value = parts
        
        record_value = record.get(col_name)

        target_str = raw_target_value.strip("'").strip('"')
        
        try:
            if isinstance(record_value, (int, float)):
                target_value = float(target_str)
            elif isinstance(record_value, bool):
                target_value = target_str.lower() in ('true', '1')
            else:
                target_value = target_str
                
        except ValueError:
            target_value = target_str 
            
        if operator == '==':
            return record_value == target_value
        elif operator == '!=':
            return record_value != target_value
        elif operator == '>':
            return record_value is not None and record_value > target_value
        elif operator == '<':
            return record_value is not None and record_value < target_value
        elif operator == '>=':
            return record_value is not None and record_value >= target_value
        elif operator == '<=':
            return record_value is not None and record_value <= target_value
        else:
            raise ValueError(f"Opérateur non supporté: {operator}")
    


    def select_data(self, table_name: str, condition_list: str = None, columns: str = "*"):
        if not self.CURRENT_DB:
            print("❌ Erreur: Aucune base de données sélectionnée.")
            return

        if table_name not in self.schemas:
            print(f"❌ Erreur: La table '{table_name}' n'existe pas.")
            return
        all_records = self._read_data(table_name)
        if condition_list:

            if len(condition_list) % 2 == 0:
                raise SyntaxError(f"Les conditions sont manquant. voir 'HELP'")

            conds_list = condition_list[::2]
            logical_operands = condition_list[1::2]

            starting_records = all_records
            result = []
            for i,cond in enumerate(conds_list):
                if i != 0:
                    print(f"i = {i} et i-1 = {i-1}")
                    if logical_operands[i-1].lower() == "or":
                        starting_records = all_records
                    elif logical_operands[i-1].lower() == "and":
                        starting_records = result
                    if cond:
                        keys = starting_records[0].keys()
                        parts = cond.split(":")
                        if parts[0] not in keys:
                            raise ValueError(f"Colonne inconnu: {parts[0]}.")
                        try:
                            filtered_records = [
                                record for record in starting_records 
                                if self._evaluate_condition(record, parts)
                            ]
                        except ValueError as e:
                            print(f"❌ Erreur de syntaxe de la condition WHERE : {e}")
                            return
                    else:
                        filtered_records = starting_records
                    if logical_operands[i-1].lower() == "or":
                        result.extend(filtered_records)
                    elif logical_operands[i-1].lower() == "and":
                        result = filtered_records
                else:
                    if cond:
                        keys = starting_records[0].keys()
                        parts = cond.split(":")
                        if parts[0] not in keys:
                            raise ValueError(f"Colonne inconnu: {parts[0]}.")
                        try:
                            result = [
                                record for record in starting_records 
                                if self._evaluate_condition(record, parts)
                            ]
                        except ValueError as e:
                            print(f"❌ Erreur de syntaxe de la condition WHERE : {e}")
                            return
                    else:
                        result = starting_records
        else:
            result = all_records

                

        if not result:
            print(f"Aucun enregistrement trouvé pour la table '{table_name}' correspondant à la condition.")
            return

        schema = self.schemas[table_name]
        column_names = [field['column'] for field in schema['fields']]

        
        if columns != "*":
            filter = []
            columns_list = columns.split(",")
            for column in columns_list:
                if column not in column_names:
                    raise KeyError(f"Colonne inconnue : {column}")
            for res in result:
                record = {}
                for column in columns_list:
                    record[column] = res[column]
                filter.append(record)
            result = filter
            column_names = columns_list
        self.display_result(column_names, result)
        


    def update_data(self, table_name, update_clause):
        # Modifie les enregistrements correspondant à la clause WHERE
        pass
        
    def delete_data(self, table_name, condition_list):
        if not self.CURRENT_DB:
            print("❌ Erreur: Aucune base de données sélectionnée.")
            return

        if table_name not in self.schemas:
            print(f"❌ Erreur: La table '{table_name}' n'existe pas.")
            return
        
        all_records = self._read_data(table_name)
        if condition_list:

            if len(condition_list) % 2 == 0:
                raise SyntaxError(f"Les conditions sont manquant. voir 'HELP'")

            conds_list = condition_list[::2]
            logical_operands = condition_list[1::2]

            starting_records = all_records
            result = []
            for i,cond in enumerate(conds_list):
                if i != 0:
                    if logical_operands[i-1].lower() == "or":
                        starting_records = all_records
                    elif logical_operands[i-1].lower() == "and":
                        starting_records = result
                    if cond:
                        keys = starting_records[0].keys()
                        parts = cond.split(":")
                        if parts[0] not in keys:
                            raise ValueError(f"Colonne inconnu: {parts[0]}.")
                        try:
                            filtered_records = [
                                record for record in starting_records 
                                if self._evaluate_condition(record, parts)
                            ]
                        except ValueError as e:
                            print(f"❌ Erreur de syntaxe de la condition WHERE : {e}")
                            return
                    else:
                        filtered_records = starting_records
                    if logical_operands[i-1].lower() == "or":
                        result.extend(filtered_records)
                    elif logical_operands[i-1].lower() == "and":
                        result = filtered_records
                else:
                    if cond:
                        keys = starting_records[0].keys()
                        parts = cond.split(":")
                        if parts[0] not in keys:
                            raise ValueError(f"Colonne inconnu: {parts[0]}.")
                        try:
                            result = [
                                record for record in starting_records 
                                if self._evaluate_condition(record, parts)
                            ]
                        except ValueError as e:
                            print(f"❌ Erreur de syntaxe de la condition WHERE : {e}")
                            return
                    else:
                        result = starting_records
            new_data = []
            for record in all_records:
                if record not in result:
                    new_data.append(record)
            self._write_data(table_name, new_data)
        else:
            self._write_data(table_name, [])



db_core = DBCore()