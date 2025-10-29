import json
import os
import getpass

class User:
    def __init__(self):
        self.user = None
        self.user_permission = None


    def user_log_in(self):
        user_name = input("Entrer votre username: ")
        user_password = getpass.getpass("password: ")
        

        if self.valid_user(user_name, user_password):
            self.user = user_name
            self.user_permission = self.get_permission(self.user)
        else:
            print("Ulisateur ou mot de passe incorrecte.")
            self.user_log_in()

    def valid_user(self, user_name: str, user_password: str) -> bool:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'user', 'users.json')

        try:
            with open(path, 'r') as f:
                data_users = json.load(f)
            
            for user in data_users:
                name = user['name']
                password = user['password']
                if name == user_name and password == user_password:
                    return True
            return False
        except Exception as e:
            print(e)

    def get_permission(self, user_name: str) -> list:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'user', 'users.json')
        if user_name != "root":

            try:
                with open(path, 'r') as f:
                    data_users = json.load(f)
                for u in data_users:
                    if u["name"] == user_name:
                        perms = u["permissions"]
                return perms
            except Exception as e:
                print(e)
        else:
            return ["ALL"]
    

    def has_permission(self, database : str, perm : str) -> bool:
        if self.user_permission[0] != "ALL":
            for base in self.user_permission:
                permissions = base.get(database, "")
                if perm in permissions:
                    return True
            return False
        else:
            return True
        

    def get_all_user(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'user', 'users.json')

        try:
            with open(path, "r") as f:
                all_user = json.load(f)
            return all_user
        except Exception as e:
            print(e)

    def write_all_user(self, all_user):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'user', 'users.json')

        try:
            with open(path, "w") as f:
                json.dump(all_user, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(e)        


    def create_new(self, user_name: str, password: str):
        all_user = self.get_all_user()
        for user in all_user:
            name = user["name"]
            if name == user_name:
                raise Exception(f"L utilisateur {user_name} existe deja")
        new =  {
            "name" : user_name,
            "password": password,
            "permissions": []
        }
        all_user.append(new)
        self.write_all_user(all_user)
        print(f"Utilisatuer {user_name} cree.")

    def switch_user(self, user_name: str):
        all_user = self.get_all_user()
        for user in all_user:
            name = user["name"]
            password = user["password"]     
            if name == user_name:
                user_password = getpass.getpass("password: ")
                if user_password == password:
                    self.user = user_name
                    self.user_permission = self.get_permission(self.user)
                return
         

        raise Exception(f"L utilisateur {user_name} n existe deja pas")
        

user = User()