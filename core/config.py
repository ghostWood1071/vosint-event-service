from typing import List, Any

from pydantic import AnyHttpUrl, BaseSettings

import sys
import os
from decouple import Config, RepositoryEnv

script_path = os.path.abspath(sys.argv[0])
script_directory = os.path.dirname(script_path)

# class Settings(BaseSettings):
class Settings:
    # APP_TITLE: str = "V-OSINT API"
    APP_ORIGINS = [
        "http://localhost:5173",
        "http://127.0.0.1:2000",
        "http://118.70.48.144:2000",
        "http://192.168.1.101:2000",
        "http://118.70.52.237:2000",
    ]
    APP_TITLE: str = "V-OSINT INGESTION"
    # APP_ORIGINS: List[AnyHttpUrl] =["*"]

    APP_HOST: str = ""
    APP_PORT: int = 6100
    APP_STATIC_DIR: str = script_directory+"/static"

    # PRIVATE_KEY: str
    # PUBLIC_KEY: str

    MONGO_DETAILS: str = ""
    DATABASE_NAME: str  = ""

    #Mongo
    # MONGO_DETAILS: str = ""
    # DATABASE_NAME: str =  ""
    
    mong_host:str = ""
    mongo_port: int = ""
    mongo_username: str = ""
    mongo_passwd: str = ""
    mongo_db_name: str = ""

    ROOT_PATH: str = ""

    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 3100
    APP_STATIC_DIR: str = "/home/ds1/vosint/v-osint-backend/static"

    PRIVATE_KEY: str = ""
    PUBLIC_KEY: str = ""

    MONGO_DETAILS: str = "mongodb://127.0.0.1:27017"
    DATABASE_NAME: str = "v-osint"

    ROOT_PATH: str = "./"

    # class Config:
    #     env_file = ".env"
    #     env_file_encoding = "utf-8"
    #     secrets_dir = "/home/ds1/vosint/v-osint-backend/secrets"
    #     case_sensitive = True

    # class Config:
    #     # print('ddddddddddddddddddddddddd',script_directory)
    #     env_file = script_directory+"/.env"
    #     env_file_encoding = "utf-8"
    #     # secrets_dir = script_directory+"/secrets"
    #     case_sensitive = False

    #
    API_PIPELINE: str = ""
    #kafka
    KAFKA_CONNECT: str = ""
    #elastic
    ELASTIC_CONNECT: str = ""
    #trans
    TRANS_CONNECT_EN: str = ""
    TRANS_CONNECT_RU: str = ""
    TRANS_CONNECT_CN: str = ""

    EXTRACT_API: str = ""

    SUMMARIZE_API: str = ""

    TRANSlATE_API: str = ""
    
    def dict(self):
        data = {k: self.__getattribute__(k) for k in self.__annotations__.keys()}
        return data

    def __dict__(self):
        data = {k: self.__getattribute__(k) for k in self.__annotations__.keys()}
        return data.items()
    
    def load_env(self):
       config = Config(RepositoryEnv(f"{script_directory}/.env"))
       for env_name in list(self.__annotations__.keys()):
            type_obj = self.__annotations__[env_name]
            value = config.get(env_name, None)
            if not value:
                continue
            if type_obj != List[str]:
                env_val = type_obj(value)
            else:
                env_val = str(value)
            self.__setattr__(env_name, env_val)

    def __init__(self):
        self.load_env()
        setting_dict = self.dict()
        print(setting_dict)
        for env_name in list(self.__annotations__.keys()):
            type_obj = self.__annotations__[env_name]
            if type_obj != List[str]:
                env_val = type_obj(os.environ.get(env_name, setting_dict.get(env_name)))
            else:
                env_val = os.environ.get(env_name, str(setting_dict.get(env_name)))
            self.__setattr__(env_name, env_val)


settings = Settings()