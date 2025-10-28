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

    def get_permission(self, user: str) -> list:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'user', 'users.json')
        if user != "root":

            try:
                with open(path, 'r') as f:
                    data_users = json.load(f)

                for user in data_users:
                    perms = user["permisson"]
                return perms
            except Exception as e:
                print(e)
        else:
            return ["ALL"]
    

    def has_permision(self, database : str, perm : str) -> bool:
        if self.user_permission[0] != "ALL":
            for base in self.user_permission:
                permissions = base.get(database, "")
                if perm in permissions:
                    return True
            return False
        else:
            return True


user = User()