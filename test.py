import socket
import msgpack

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = 8080  # The port used by the server


class Table:
    def __init__(self):
        self.column = []
        self.column_types = []

    def add_row(self, row: list):
        if len(row) != len(self.column_types):
            raise ValueError("row length does not match table column length")

        for i, value in enumerate(row):
            if type(value) is self.column_types[i]:
                raise TypeError("row value type does not match table column type")

            self.column[i].append(value)

    def add_column(self, column: list, column_type: type):

        for value in column:
            if type(value) is not column_type:
                raise TypeError("column value type does not match table column type")

        self.column.append(column)

    def add_column_no_checks(self, column: list):
        self.column.append(column)

    def display(self):
        print("Table object:")
        for column_type in self.column_types:
            print(f"|\t{column_type.__name__}\t", end=" ")
        print()

        for row in range(len(self.column[0])):
            for column in range(len(self.column)):
                print(f"|\t{self.column[column][row]} \t", end=" ")
            print()


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))

    payload = b"select table 0"

    s.sendall(len(payload).to_bytes(4, byteorder="big"))
    s.sendall(payload)

    print(f"sent query: '{payload.decode('utf-8')}'")

    inc_payload_size = s.recv(4)
    converted_payload_size = int.from_bytes(inc_payload_size, byteorder="big")
    # print(converted_payload_size)

    inc_payload = s.recv(converted_payload_size)
    # print(inc_payload)

    converted = msgpack.unpackb(inc_payload, raw=False)
    table = Table()

    for (idx, given_type) in enumerate(converted[1]):
        match given_type:
            case "Int":
                table.column_types.append(int)
                table.add_column_no_checks(converted[0][idx]["Int"])
            case "Float":
                table.column_types.append(float)
                table.add_column_no_checks(converted[0][idx]["Float"])
            case "String":
                table.column_types.append(str)
                table.add_column_no_checks(converted[0][idx]["String"])
            case "Bool":
                table.column_types.append(bool)
                table.add_column_no_checks(converted[0][idx]["Bool"])
            case _:
                raise ValueError("unknown type")

    table.display()
