from db_core import db_core 


DB_NAME = "realDB"

def parse_command(command: str) -> tuple[str, list[str]]:
    if not command:
        return "", []
    
    parts = command.strip().split()
    
    action = parts[0].upper()
    
    args = parts[1:]
    
    return action, args

def execute_command(action: str, args: list[str]) -> bool:
    
    if action == "QUIT" or action == "EXIT":
        print(f"Fermeture de {DB_NAME}.")
        return False
    
    if action == "HELP":
        help_message = (
           "Commandes disponibles :\n"
            " \n"
            "    QUIT or EXIT............. Quitte l'application.\n"
            "    HELP .................... Affiche cette aide.\n"
            " \n"
            "    SHOW DATABASES .......... Liste toutes les bases de données.\n"
            "    CREATE DATABASE <nom> ... Crée un nouveau dossier de base de données.\n"
            "    DROP DATABASE <nom> ...........Supprimer une database.\n"
            "    USE <db_name> ........... Sélectionne la base de données active.\n"
            " \n"
            "    SHOW TABLES ............. Liste toutes les tables de la DB active.\n"
            "    DESCRIBE <table_name> ......Liste toutes les champq de la table.\n"
            "    DROP TABLE <nom> ...........Supprimer une table.\n"
            "    CREATE TABLE <nom> <champs> Crée une nouvelle table (ex: id:int:pk name:str).\n"
            "    INSERT INTO <table_nom> <valeurs> Insère une ligne dans la table.\n"
            "    SELECT * FROM <table_nom> .. Affiche toutes les données de la table.\n"
            "    SELECT * FROM <table_nom> WHERE <condition>.. Affiche toutes les données de la table.\n"
            "    UPDATE <nom> SET <col> = <val> WHERE <cond> Modifie des lignes.\n"
            "    DELETE FROM <nom> WHERE <cond> Supprime des lignes.\n"
            " \n"
        )
        print(help_message)
        return True
    
    
    try:
        if action == "USE" and len(args) == 1:
            db_core.use_db(args[0])

        elif action == "CREATE" and len(args) >= 2:
            if args[0].upper() == "DATABASE":
                database_name = args[1]
                db_core.create_database(database_name)
            elif args[0].upper() == "TABLE":
                table_name = args[1]
                fields_def = args[2:]
                db_core.create_table(table_name, fields_def)
            else:
                print(f" Erreur: Commande non reconnue ou syntaxe incorrecte: {action}")
        
        
        elif action == "DROP" and len(args) == 2:
            if args[0].upper() == "DATABASE":
                database_name = args[1]
                db_core.drop_database(database_name)
            elif args[0].upper() == "TABLE":
                table_name = args[1]
                db_core.drop_table(table_name)
            else:
                print(f" Erreur: Commande non reconnue ou syntaxe incorrecte: {action}")

        elif action == "INSERT" and len(args) >= 3 and args[0].upper() == "INTO":
            table_name = args[1]
            values = args[2:]
            db_core.insert_data(table_name, values)
            
        elif action == "SELECT":
            if len(args) == 3 and args[1].upper() == "FROM":
                table_name = args[2]
                db_core.select_data(table_name, "", args[0])
            elif len(args) >= 5  and args[1].upper() == "FROM" and args[3].upper() == "WHERE":
                condition = args[4:]
                table_name = args[2]
                db_core.select_data(table_name, condition, args[0])
            else:
                print("Erreur de syntaxe: SELECT * FROM <table_name>")

        elif action == "SHOW" and len(args) == 1:
            if args[0].upper() == "DATABASES":
                db_core.show_databases()
            elif args[0].upper() == "TABLES":
                db_core.show_tables()
            else:
                print(f" Erreur: Commande non reconnue ou syntaxe incorrecte: {action}")
        
        elif action == "DESCRIBE" and len(args) == 1:
            db_core.describe_table(args[0])
            
        else:
            print(f" Erreur: Commande non reconnue ou syntaxe incorrecte: {action}")

    except Exception as e:
        print(f"Erreur lors de l'exécution de la commande: {e}")
        
    return True

def start_cli():

    print(f"Démarrage du {DB_NAME} - Tapez 'HELP' pour les commandes.")
    db_core.load_db()
    
    
    running = True
    while running:
        try:
            
            database = db_core.CURRENT_DB+">" if db_core.CURRENT_DB else ""
            user_input = input(f"{DB_NAME}>{database} ").strip()
            
            if not user_input:
                continue

            action, args = parse_command(user_input)
            running = execute_command(action, args)
            
        except EOFError:
            print("\nAu revoir! (CTRL+D)")
            break
        except KeyboardInterrupt:
            print("\n Au revoir! (CTRL+C)")
            break


if __name__ == "__main__":
    start_cli()