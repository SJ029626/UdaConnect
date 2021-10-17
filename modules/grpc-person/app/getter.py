import grpc
import grpc_person_pb2
import grpc_person_pb2_grpc



print("Sending The Sample Payload...")
channel = grpc.insecure_channel("localhost:5005")
stub = grpc_person_pb2_grpc.PersonServiceStub(channel)

response = stub.Get(grpc_person_pb2.Empty())
print(response)
