import socket
import pymongo
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import uuid
import struct



class TCPServer:

    def __init__(self, address='0.0.0.0', port=54321, db_uri='mongodb://172.16.0.125:27017/'):
        self.server_address = (address, port)
        self.db_uri = db_uri
        self.setup_logging()
        self.setup_db()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(self.server_address)
        self.server_socket.listen(1000)
        # print(f'Server is listening on {address}:{port}')
        self.logger.info(f'Server is listening on {address}:{port}')

    def setup_logging(self):

        self.logger = logging.getLogger('TCPServer')
        self.logger.setLevel(logging.INFO)
        handler = RotatingFileHandler(
            'server.log', maxBytes=10 * 1024 * 1024, backupCount=5)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def setup_db(self):
        self.client = pymongo.MongoClient(self.db_uri)
        self.db = self.client['pms']

    def run(self):
        try:
            while True:
                print('Waiting for a connection...')
                self.logger.info('Waiting for a connection...')
                client_socket, client_address = self.server_socket.accept()
                with client_socket:
                    self.logger.info(f'Connection from {client_address}')
                    self.handle_client(client_socket)
        except Exception as e:
            print(f'An error occurred: {e}')
            self.logger.error(f'An error occurred: {e}')
        finally:
            self.server_socket.close()

    def handle_client(self, client_socket):
        try:
            data = client_socket.recv(1024)
            if data:
                # print(f'Data Received - Actual: {str(data)}')
                self.logger.info(f'Data Received - Actual: {str(data)}')
                data_str = str(data.decode('utf-8'))
                print(data_str)
                self.logger.info(f'Data Received - Decode: {data_str}')
                # print(f'Data Received - Decode: {str(data_str)}')
                if "ID:001" in data_str:
                    # print("ID:001")
                    parse_data_energy = self.parse_data_energy(data_str)
                    print(parse_data_energy)
                    self.save_to_db('data_energy', parse_data_energy)
                    self.logger.info(
                        f'Received and saved data: {parse_data_energy}')

                if "RTU_D" in data_str:
                    # print("ID:225")
                    if "RS" in data_str and "CU_D" not in data_str:

                        parsed_data_ios = self.parse_data_io(data_str)
                        print(parsed_data_ios)
                        self.save_to_db('data_io', parsed_data_ios)
                        self.logger.info(
                            f'Received and saved data: {parsed_data_ios}')
                    elif "CU_D" in data_str:
                        pass

                        # parsed_data_ups_io = self.parse_data_io_and_ups(data_str)
                        # print(parsed_data_ups_io)
                    # self.save_to_db('data_ups', parsed_data_ups_io)
                    # self.logger.info(f'Received and saved data: {parsed_data_ups_io}')

        except Exception as e:
            self.logger.error(f'Error handling client: {e}')

    def parse_data_io(self, data):
        print("parse_data_io")
        data_spilt = data.split("\r\n")
        # print("input-data")
        # print(data_spilt)
        # print("Array count : " + str(len(data_spilt)))
        ip_address = None
        device_id = None
        for value in data_spilt:
            # print(value)
            if value is not None and value != "" and "HOST" in value:
                ip_address = value.replace("HOST: ", "")
            elif "CU_D" not in value and value is not None and "RS" in value:
                value_split = value.split("#")
                # print(value_split)
                if len(value_split) >= 3:
                    device_id = value_split[0][1:11]
                    print("device_id:", device_id)
                    machine_id = value_split[0][15:18]
                    print("machine_id:", machine_id)
                    analog_datas = value_split[2].replace(">", "").split(",")
                    print(analog_datas)
                    digital_data1 = analog_datas[0][9]
                    print("digital_data1:", digital_data1)
                    digital_data2 = analog_datas[0][10]
                    print("digital_data2:", digital_data2)
                    digital_data3 = analog_datas[0][11]
                    print("digital_data3:", digital_data3)
                    digital_data4 = analog_datas[0][12]
                    print("digital_data4:", digital_data4)
                    digital_data5 = analog_datas[0][13]
                    print("digital_data5:", digital_data5)
                    digital_data6 = analog_datas[0][14]
                    print("digital_data6:", digital_data6)
                    digital_data7 = analog_datas[0][15]
                    print("digital_data7:", digital_data7)
                    digital_data8 = analog_datas[0][16]
                    print("digital_data8:",digital_data8)
                    A1 = analog_datas[1][3:9]
                    print("anlog_data1:", A1)
                    A2 = analog_datas[1][10:16]
                    print("anlog_data2:", A2)
                    A3 = analog_datas[1][17:23]
                    print("anlog_data3:", A3)
                    A4 = analog_datas[1][24:30]
                    print("anlog_data4:", A4)
                    A5 = analog_datas[1][31:37]
                    print("anlog_data5:", A5)
                    A6 = analog_datas[1][38:44]
                    print("anlog_data6:", A6)
                    A7 = analog_datas[1][45:51]
                    print("anlog_data7:", A7)
                    A8 = analog_datas[1][52:58]
                    print("anlog_data8:", A8)
                    RS1 = analog_datas[2][3]
                    print("relay_status1:", RS1)
                    RS2 = analog_datas[2][4]
                    print("relay_status2:", RS2)
                    RS3 = analog_datas[2][5]
                    print("relay_status3:", RS3)
                    RS4 = analog_datas[2][6]
                    print("relay_status4:", RS4)
                    RS5 = analog_datas[2][7]
                    print("relay_status5:", RS5)
                    RS6 = analog_datas[2][8]
                    print("relay_status6:", RS6)
                    RS7 = analog_datas[2][9]
                    print("relay_status7:", RS7)
                    RS8 = analog_datas[2][10]
                    print("relay_status8:", RS8)
                    created_on = datetime.now().replace(microsecond=0).isoformat()
                    print("created_on:", created_on),
                    updated_on = datetime.now().replace(microsecond=0).isoformat()
                    print("updated_on:", updated_on)

                    parsed_data = {
                        "_id": uuid.uuid4().hex,
                        "device_id": device_id,
                        "machine_id": machine_id,
                        "digital_data1": digital_data1,
                        "digital_data2": digital_data2,
                        "digital_data3": digital_data3,
                        "digital_data4": digital_data4,
                        "digital_data5": digital_data5,
                        "digital_data6": digital_data6,
                        "digital_data7": digital_data7,
                        "digital_data8": digital_data8,
                        "anlog_data1": A1,
                        "anlog_data2": A2,
                        "anlog_data3": A3,
                        "anlog_data4": A4,
                        "anlog_data5": A5,
                        "anlog_data6": A6,
                        "anlog_data7": A7,
                        "anlog_data8": A8,
                        "relay_status1": RS1,
                        "relay_status2": RS2,
                        "relay_status3": RS3,
                        "relay_status4": RS4,
                        "relay_status5": RS5,
                        "relay_status6": RS6,
                        "relay_status7": RS7,
                        "relay_status8": RS8,
                        "created_on": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "updated_on": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "ip_address": ip_address
                    }
                    return parsed_data

    def parse_data_io_and_ups(self, data):
        print("parse_data_io_and_ups")
        data_spilt = data.split("\r\n")
        print(data_spilt)
        print("Array count : " + str(len(data_spilt)))
        ip_address = None
        device_id = None
        for value in data_spilt:
            print(value)
            if value is not None and value != "" and "HOST" in value:
                ip_address = value.replace("HOST: ", "")
            elif "CU_D" not in value and value is not None and "RS" in value:
                value_split = value.split("#")
                # print(value_split)
                if len(value_split) >= 3:
                    device_id = value_split[0][1:11]
                    print("device_id:", device_id)
                    machine_id = value_split[0][15:18]
                    print("machine_id:", machine_id)
                    analog_datas = value_split[2].replace(">", "").split(",")
                    # print(analog_datas)
                    digital_data1 = analog_datas[0][9]
                    print("digital_data1:", digital_data1)
                    digital_data2 = analog_datas[0][10]
                    print("digital_data2:", digital_data2)
                    digital_data3 = analog_datas[0][11]
                    print("digital_data3:", digital_data3)
                    digital_data4 = analog_datas[0][12]
                    print("digital_data4:", digital_data4)
                    digital_data5 = analog_datas[0][13]
                    print("digital_data5:", digital_data5)
                    digital_data6 = analog_datas[0][14]
                    print("digital_data6:", digital_data6)
                    digital_data7 = analog_datas[0][15]
                    print("digital_data7:", digital_data7)
                    digital_data8 = analog_datas[0][16]
                    print("digital_date8:",digital_data8)
                    A1 = analog_datas[1][3:9]
                    print("anlog_data1:", A1)
                    A2 = analog_datas[1][10:16]
                    print("anlog_data2:", A2)
                    A3 = analog_datas[1][17:23]
                    print("anlog_data3:", A3)
                    A4 = analog_datas[1][24:30]
                    print("anlog_data4:", A4)
                    A5 = analog_datas[1][31:37]
                    print("anlog_data5:", A5)
                    A6 = analog_datas[1][38:44]
                    print("anlog_data6:", A6)
                    A7 = analog_datas[1][45:51]
                    print("anlog_data7:", A7)
                    A8 = analog_datas[1][52:58]
                    print("anlog_data8:", A8)
                    created_on = datetime.now()
                    print("created_on:", created_on),
                    updated_on = datetime.now(),
                    print("updated_on:", updated_on)

                    parsed_data = {
                        "_id": uuid.uuid4().hex,
                        "device_id": device_id,
                        "machine_id": machine_id,
                        "digital_data1": digital_data1,
                        "digital_data2": digital_data2,
                        "digital_data3": digital_data3,
                        "digital_data4": digital_data4,
                        "digital_data5": digital_data5,
                        "digital_data6": digital_data6,
                        "digital_data7": digital_data7,
                        "digital_data8": digital_data8,
                        "anlog1": A1,
                        "anlog2": A2,
                        "anlog3": A3,
                        "anlog4": A4,
                        "anlog5": A5,
                        "anlog6": A6,
                        "anlog7": A7,
                        "anlog8": A8,
                        # "relay_status1": RS1,
                        # "relay_status2": RS2,
                        # "relay_status3": RS3,
                        # "relay_status4": RS4,
                        # "relay_status5": RS5,
                        # "relay_status6": RS6,
                        # "relay_status7": RS7,
                        # "relay_status8": RS8,
                        # "current_status1": CU1,
                        # "current_status2": CU2,
                        # "current_status3": CU3,
                        # "current_status4": CU4,
                        # "current_status5": CU5,
                        # "current_status6": CU6,
                        # "current_status7": CU7,
                        "created_on": datetime.now().replace(microsecond=0).isoformat(),
                        "updated_on": datetime.now().replace(microsecond=0).isoformat(),
                        "ip_address": ip_address
                    }

                    return parsed_data

    def parse_data_energy(self, data):
        # print(f'Data Received - data: {str(data)}')
        # original_String = data
        # print(f'Data Received - original_String: {str(original_String)}')
        data_spilt = data.split("\r\n")
        # print(data_spilt)
        # print("Array count : " + str(len(data_spilt)))
        ip_address = None
        device_id = None
        for value in data_spilt:
            print(value)
            if value is not None and value != "" and "HOST" in value:
                ip_address = value.replace("HOST: ", "")
            elif value is not None and value != "" and "ID:001" in value:
                value_split = value.split("#")
                print(value_split)
                if len(value_split) >= 3:
                    device_id = value_split[0][1:11]
                    print("device id:", device_id)
                    machine_id = value_split[0][15:18]
                    print("machine_id:", machine_id)
                    analog_datas = value_split[2].replace(">", "").split(",")
                    print(analog_datas)
                    if len(analog_datas) >= 28:
                        total_active_energy = analog_datas[0] + analog_datas[1]
                        int_value1 = int(total_active_energy, 16)
                        float_value1 = struct.unpack('!f', struct.pack('!I', int_value1))[0]
                        # print(float_value1)    #convert hex to int then int to float
                        print("total_active_energy:", float_value1)
                        import_active_energy = analog_datas[2] + analog_datas[3]
                        int_value2 = int(import_active_energy, 16)
                        float_value2 = struct.unpack('!f', struct.pack('!I', int_value2))[0]
                        print("import_active_energy:", float_value2)
                        export_active_energy = analog_datas[4] + analog_datas[5]
                        int_value3 = int(export_active_energy, 16)
                        float_value3 = struct.unpack('!f', struct.pack('!I', int_value3))[0]
                        print("export_active_energy:", float_value3)
                        total_reactive_energy = analog_datas[6]+analog_datas[7]
                        int_value4 = int(total_reactive_energy, 16)
                        float_value4 = struct.unpack('!f', struct.pack('!I', int_value4))[0]
                        print("total_reactive_energy:", float_value4)
                        import_reactive_energy = analog_datas[8] + analog_datas[9]
                        int_value5 = int(import_reactive_energy, 16)
                        float_value5 = struct.unpack('!f', struct.pack('!I', int_value5))[0]
                        print("import_reactive_energy:",float_value5)
                        export_reactive_energy = analog_datas[10] + analog_datas[11]
                        int_value6 = int(export_reactive_energy, 16)
                        float_value6 = struct.unpack('!f', struct.pack('!I', int_value6))[0]
                        print("export_reactive_energy:",float_value6)
                        apparent_energy = analog_datas[12] + analog_datas[13]
                        int_value7 = int(apparent_energy, 16)
                        float_value7 = struct.unpack('!f', struct.pack('!I', int_value7))[0]
                        print("apparent_energy:", float_value7)
                        active_power = analog_datas[14] + analog_datas[15]
                        int_value8 = int(active_power, 16)
                        float_value8 = struct.unpack('!f', struct.pack('!I', int_value8))[0]
                        print("active_power:", float_value8)
                        reactive_power = analog_datas[16] + analog_datas[17]
                        int_value9 = int(reactive_power, 16)
                        float_value9 = struct.unpack('!f', struct.pack('!I', int_value9))[0]
                        print("reactive_power:", float_value9)
                        apparent_power = analog_datas[18] + analog_datas[19]
                        int_value10 = int(apparent_power, 16)
                        float_value10 = struct.unpack('!f', struct.pack('!I', int_value10))[0]
                        print("apparent_power:", float_value10)
                        voltage = analog_datas[20] + analog_datas[21]
                        int_value11 = int(voltage, 16)
                        float_value11 = struct.unpack('!f', struct.pack('!I', int_value11))[0]
                        print("voltage:",float_value11)
                        current = analog_datas[22] + analog_datas[23]
                        int_value12 = int(current, 16)
                        float_value12 = struct.unpack('!f', struct.pack('!I', int_value12))[0]
                        print("current:", float_value12)
                        power_factory = analog_datas[24] + analog_datas[25]
                        int_value13 = int(power_factory, 16)
                        float_value13 = struct.unpack('!f', struct.pack('!I', int_value13))[0]
                        print("power_factory:", float_value13)
                        frequency = analog_datas[26] + analog_datas[27]
                        int_value14 = int(frequency, 16)
                        float_value14 = struct.unpack('!f', struct.pack('!I', int_value14))[0]
                        print("frequency:", float_value14)
                        created_on = datetime.now().replace(microsecond=0).isoformat()
                        print("created_on:", created_on),
                        updated_on = datetime.now().replace(microsecond=0).isoformat()
                        print("updated_on:", updated_on)

                        parsed_data = {
                            "_id": uuid.uuid4().hex,
                            "device_id": device_id,
                            "machine_id": machine_id,
                            "total_active_energy": float_value1,
                            "import_active_energy": float_value2,
                            "export_active_energy": float_value3,
                            "total_reactive_energy": float_value4,
                            "import_reactive_energy ": float_value5,
                            "export_reactive_energy": float_value6,
                            "apparent_energy":float_value7,
                            "active_power": float_value8,
                            "reactive_power": float_value9,
                            "apparent_power": float_value10,
                            "voltage": float_value11,
                            "current": float_value12,
                            "power_factory": float_value13,
                            "frequency": float_value14,
                            "created_on": datetime.now().strftime("%Y-%m-%d %H:%M"),
                            "updated_on": datetime.now().strftime("%Y-%m-%d %H:%M"),
                            "ip_address": ip_address
                        }
                        return parsed_data

    def save_to_db(self, collection_name, data):
        collection = self.db[collection_name]
        collection.insert_one(data)


if __name__ == '__main__':
    server = TCPServer()
    server.run()
