from db_core import db_core
from user import user


DB_NAME = "realDB"
PERMISSION  = {}

def parse_command(command: str) -> tuple[str, list[str]]:
    if not command:
        return "", []
    
    parts = command.strip().split()
    
    action = parts[0].upper()
    
    args = parts[1:]
    
    return action, args

def execute_command(action: str, args: list[str]) -> bool:
    
    global PERMISSION
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
            "    GRANT/REVOKE <CREATE/READ/DELETE> ON <DATABASE> TO <USER>......Modifier les permmissions des utilisateurs sur une base."
        )
        print(help_message)
        return True
    
    
    try:
        if action == "USE" and len(args) == 1:
            db_core.use_db(args[0])
            PERMISSION = get_perms()

        elif action == "CREATE" and len(args) >= 2:
            if args[0].upper() == "DATABASE":
                if user.user == "root":
                    database_name = args[1]
                    db_core.create_database(database_name)
                else:
                    print("Permission non accordé.")
            elif args[0].upper() == "USER" and args[2].upper() == "IDENTIFIED" and args[3].upper() == "BY":
                if user.user == "root":
                    user_name = args[1]
                    password = args[4]
                    user.create_new(user_name, password)
                else:
                    print("Permission non accordé.")

            elif args[0].upper() == "TABLE":
                if PERMISSION["c"]:
                    table_name = args[1]
                    fields_def = args[2:]
                    db_core.create_table(table_name, fields_def)
                else:
                    print("Permission non accordé.")
            else:
                print(f" Erreur: Commande non reconnue ou syntaxe incorrecte: {action}")
        

        elif action == "DELETE":
            if PERMISSION["d"]:
                if len(args) == 3 and args[0] == "*" and args[1].upper() == "FROM":
                    table_name = args[2]
                    db_core.delete_data(table_name, "")
                elif len(args)>3 and args[0].upper() == "FROM" and args[2].upper() == "WHERE":
                    table_name = args[1]
                    condition = args[3:]
                    db_core.delete_data(table_name, condition)
                else:
                    print("Errreur de synthaxe : DELETE * FROM <TABLE> or DELETE FROM <TABLE> WHERE <CONDITION>")
            else:
                print("Permission non accordé.")
        
        elif action == "GRANT":
            if len(args) == 5 and args[1].upper() == "ON" and args[3].upper() == "TO":
                database_name = args[2]
                user_name = args[4]
                perm = args[0]
                user.grant_perms(database_name, user_name, perm, db_core.DATABASES)
            else:
                print("Erreur de synthaxe. GRANT <PERMISSION> ON <DATABASE> TO <USER>.")

        
        elif action == "REVOKE":
            if len(args) == 5 and args[1].upper() == "ON" and args[3].upper() == "TO":
                database_name = args[2]
                user_name = args[4]
                perm = args[0]
                user.revoke_perm(database_name, user_name, perm, db_core.DATABASES)
            else:
                print("Erreur de synthaxe. REVOKE <PERMISSION> ON <DATABASE> TO <USER>.")
            
        
        
        elif action == "DROP" and len(args) == 2:
            if args[0].upper() == "DATABASE":
                if user.user == "root":
                    database_name = args[1]
                    db_core.drop_database(database_name)
                else:
                    print("Permission non accordé.")
            elif args[0].upper() == "TABLE":
                if PERMISSION["d"]:
                    table_name = args[1]
                    db_core.drop_table(table_name)
                else:
                    print("Permission non accordé.")
            else:
                print(f"Erreur de synthaxe : DROP TABLE/DATABASE <DATABASENAME/TABLENAME>")


        elif action == "INSERT" and len(args) >= 3 and args[0].upper() == "INTO":
            if PERMISSION["c"]:
                table_name = args[1]
                values = args[2:]
                db_core.insert_data(table_name, values)
            else:
                print("Permission non accordé.")

        elif action == "SELECT":
            if PERMISSION["r"]:
                if len(args) == 3 and args[1].upper() == "FROM":
                    table_name = args[2]
                    db_core.select_data(table_name, "", args[0])
                elif len(args) >= 5  and args[1].upper() == "FROM" and args[3].upper() == "WHERE":
                    condition = args[4:]
                    table_name = args[2]
                    db_core.select_data(table_name, condition, args[0])
                else:
                    print("Erreur de syntaxe: SELECT * FROM <table_name>")
            else:
                print("Permission non accordé.")



        elif action == "SHOW" and len(args) == 1:
            if args[0].upper() == "DATABASES":
                db_core.show_databases()
            elif args[0].upper() == "TABLES":
                if db_core.CURRENT_DB:
                    if PERMISSION["r"]:
                        db_core.show_tables()
                    else:
                        print("Permission non accordé.")
                else:
                    db_core.show_tables()        
            else:
                print(f" Erreur de synthaxe : SHOW TABLES/DATABASES")


        elif action == "SU" and len(args) == 1:
            user.switch_user(args[0])
            if db_core.CURRENT_DB:
                PERMISSION = get_perms()


        elif action == "DESCRIBE" and len(args) == 1:
            if PERMISSION["r"]:
                db_core.describe_table(args[0])
            else:
                print("Permission non accordé.")

        else:
            print(f" Erreur: Commande non reconnue ou syntaxe incorrecte: {action}")

    except Exception as e:
        print(f"Erreur lors de l'exécution de la commande: {e}")
        
    return True

def get_perms():
    create_p = user.has_permission(db_core.CURRENT_DB, "c")
    read_p = user.has_permission(db_core.CURRENT_DB, "r")
    delete_p = user.has_permission(db_core.CURRENT_DB, "d")
    
    return {"c": create_p, "r": read_p, "d": delete_p}

def start_cli():

    if not user.user:
        user.user_log_in()
    
    

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