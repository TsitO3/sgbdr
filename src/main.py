import sys
#from src import db_core 


DB_NAME = "MiniDB"

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
            "  QUIT .................... Quitte l'application.\n"
            "  HELP .................... Affiche cette aide.\n"
            "  SHOW TABLES ............. Liste toutes les tables.\n"
            "  CREATE TABLE <nom> <champs> Créer une nouvelle table (ex: id:int name:str).\n"
            "  INSERT INTO <table_nom> <valeurs> Insère une ligne dans la table.\n"
            "  SELECT * FROM <table_nom> .. Affiche toutes les données de la table."
        )
        print(help_message)
        return True
    
    
    try:
        if action == "CREATE" and len(args) >= 2 and args[0].upper() == "TABLE":
            table_name = args[1]
            fields_def = args[2:]
            db_core.create_table(table_name, fields_def)
        
        elif action == "INSERT" and len(args) >= 3 and args[0].upper() == "INTO":
            table_name = args[1]
            values = args[2:]
            db_core.insert_data(table_name, values)
            
        elif action == "SELECT":
            if len(args) == 3 and args[1].upper() == "FROM":
                results = db_core.select_all(args[2])
                print(f"Résultats pour {args[2]}:\n {results}")
            else:
                print("Erreur de syntaxe: SELECT * FROM <table_name>")

        elif action == "SHOW" and len(args) == 1 and args[0].upper() == "TABLES":
            db_core.show_tables()
            
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
            
            user_input = input(f"{DB_NAME}> ").strip()
            
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
    class MockDBCore:
        def load_db(self): print("[DB_CORE] Initialisation OK.")
        def create_table(self, name, fields): print(f"[DB_CORE] Création de la table '{name}' avec champs: {fields}")
        def insert_data(self, name, values): print(f"[DB_CORE] Insertion dans '{name}': {values}")
        def select_all(self, name): return [f"ligne 1 de {name}"]
        def show_tables(self): print("[DB_CORE] Tables: users, products")

    db_core = MockDBCore()
    start_cli()