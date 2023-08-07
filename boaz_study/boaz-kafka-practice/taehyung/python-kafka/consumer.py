import csv
import json
import os
import time

from kafka import KafkaConsumer


class Consumer:
    def __init__(self, brokers, topicName):
        self.consumer = KafkaConsumer(
            topicName,
            group_id="bank-consumer-group-v3",
            bootstrap_servers=brokers,
            api_version=(0, 11, 5),
        )

    def income_check(self):
        print("Start income check")
        new_data = []  # 새로운 데이터 저장
        for message in self.consumer:
            data = json.loads(message.value.decode())
            # print(data)
            # 종료 신호인 경우
            if data["row"][0] == "DONE":
                break
            # income이 $120K 이상인 경우
            if "$120K +" in str(data["row"][7]):
                print("--income exceeds $120K")
                # CLIENTNUM, Customer_Age, Gender, Dependent_count, Education_Level, Marital_Status, Income_Category, Card_Category 정보만 저장
                new_row = [data["row"][0], data["row"][2], data["row"][3], data["row"][4], data["row"][5], data["row"][6], data["row"][7], data["row"][8]]
                new_data.append(new_row)

                print(f'{data["index"]} {new_row.__str__()}')

                # csv 파일 업데이트
                file_name = "./high_income_customers.csv"
                file_path = os.path.join(os.path.dirname(__file__), file_name)
                with open(file_path, "a", newline='') as f:
                    writer = csv.writer(f)
                    if os.stat(file_path).st_size == 0:  # 파일이 비어있으면 헤더 추가
                        writer.writerow(['CLIENTNUM', 'Customer_Age', 'Gender', 'Dependent_count', 'Education_Level', 'Marital_Status', 'Income_Category', 'Card_Category'])
                    writer.writerow(new_row)


        print("End income check")


if __name__ == '__main__':
    brokers = ["kafka:29092"]
    topicName = "bank"
    consumer = Consumer(brokers, topicName)
    consumer.income_check()
