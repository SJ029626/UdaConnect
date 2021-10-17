import time
from concurrent import futures
import grpc
import grpc_person_pb2
import grpc_person_pb2_grpc
import requests

response = requests.get('http://34.125.83.32:30002/api-person/persons')
personss = response.json()


class PersonServicer(grpc_person_pb2_grpc.PersonServiceServicer):

    def Get(self, request, context):
        print('Server Recieved a request')
        person_list = []
        for person_dict in personss:
            person_list.append(grpc_person_pb2.PersonMsg(id=person_dict['id'],
                               first_name=person_dict['first_name'],
                               last_name=person_dict['last_name'],
                               company_name=person_dict['company_name']))

        response = grpc_person_pb2.PersonMsgList()
        response.persons.extend(person_list)

        print('Delivered the request successfully')
        return response

# Initialize gRPC server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))

grpc_person_pb2_grpc.add_PersonServiceServicer_to_server(PersonServicer(),
                                                         server)


print("Server Started on PORT 5005...")
server.add_insecure_port("[::]:5005")
server.start()
# Keep thread alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)