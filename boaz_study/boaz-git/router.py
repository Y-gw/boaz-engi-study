from fastapi import APIRouter, HTTPException
from schema import User
import pandas as pd
import os

router = APIRouter()

# 임시 데이터베이스로 사용할 리스트
users_db = list()

file_list = os.listdir("./data")
for id, file in enumerate(file_list):
    user_data = pd.read_csv("./data/" + file_list[id])
    users_db.append(User(id=id, username=user_data.username[0], group=user_data.group[0]))

@router.get("/users")
def read_user():
    user_dict = dict()
    users = [(user.username, user.group )for user in users_db]
    user_dict["엔지니어링 공동세션 참석자"] = users
    return user_dict

