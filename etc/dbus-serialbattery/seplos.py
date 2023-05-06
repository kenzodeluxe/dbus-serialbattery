# -*- coding: utf-8 -*-
# Seplos protocol implementation: https://github.com/shorawitz/Seplos/blob/main/seplos.py
# Iterates over all packs and makes relevant information available to Victron as one "virtual" battery instance
# All battery details from all packs are published to MQTT
# To-Do:
# - Address FIXME statements
# - return to generic way of using serial interface (vs. using ser.open directly)

from battery import Protection, Battery, Cell
from codecs import decode
from operator import itemgetter
from utils import *
from struct import *
import paho.mqtt.publish as publish

mqtt_msgs = []

class Seplos(Battery):
    def __init__(self, port, baud, address):
        super(Seplos, self).__init__(port, baud, address)
        self.type = "Seplos"
        self.mqtt_topic = 'seplos'
        self.mqtt_host = 'localhost'

    def convert_address(self, address):
        send = '20' + address + '4642E00201'
        sum = 0
        for i in range(len(send)):
            sum += ord(send[i])
        bwsum = ~sum + 1
        hexsum = format(bwsum, '04X').zfill(4)
        hexbwsum = hex((bwsum + (1 << 16)) % (1 << 16)).lstrip('0x')
        cmd = '~' + send + hexbwsum.upper() + '\r\n'
        return cmd

    def get_bms_information(self, data):
        battery_packs_online = len(data)
        self.cells = []
        for i in range(1,17):
            self.cells.append(Cell(False))  # FIXME: required?
        all_env_temp = []
        all_pcb_temp = []
        all_cell_temp = []
        all_pack_current = []
        all_pack_voltage = []
        all_pack_ah_available = []
        all_pack_ah_total = []
        all_pack_soc = []
        all_pack_ah_rated = []
        all_pack_cycles = []
        all_pack_soh = []
        all_pack_bus_voltage = []
        all_cell_voltages = {}
        mqtt_msgs = []
        mqtt_msgs.append({'topic':f'{self.mqtt_topic}/battery_packs_online', 'payload': len(data)})
        for pack in data:
            invalid_data = False
            offset = 19

            # Get cell voltage information
            try:
                str_return_data = str(data[pack].hex())
                str_bytes = bytes(str_return_data, encoding='utf-8')
                binary_string = decode(str_bytes, "hex")
                converted = str(binary_string, 'utf-8')
                ncell = int(converted[17:19],16)
            except Exception as e:
                logging.error(f'Did not receive valid data from battery ({e})')
                return False
            for cell in range(1, ncell+1):
                try:
                    cell_voltage = float(int(converted[offset:offset+4],16)/1000)
                    offset += 4
                    self.cells[cell-1].voltage = cell_voltage
                    # Have seen invalid cell voltages appear, needs further investigation
                    if cell_voltage > 2 and cell_voltage < 4:
                        all_cell_voltages[f'{pack}-{cell}'] = cell_voltage
                        mqtt_msgs.append({'topic': f'{self.mqtt_topic}/pack{pack}/cell{cell}/cell_voltage', 'payload': cell_voltage})
                    else:
                        logging.error(f'Cell {cell} in pack {pack} reports {cell_voltage}V, skipping current data for battery pack.')
                        invalid_data = True
                        break
                except Exception as e:
                    logging.error(f'Unable to convert data for cell voltage, skipping battery pack {pack}')
                    invalid_data = True
                    break
            # if data received for a specific pack was incorrect, skip pack
            if invalid_data:
                continue

            # Get BMS temperature information
            try:
                nprobes = int(converted[offset:offset+2],16)
                offset += 2
                for probe in range(1, nprobes+1):
                    temp = float((int(converted[offset:offset+4],16) - 2731) / 10)
                    offset += 4
                    if probe <= 4:
                        all_cell_temp.append(temp)
                        mqtt_msgs.append({'topic': f'{self.mqtt_topic}/pack{pack}/temp{probe}/temp', 'payload': temp})
                    elif probe == 5:
                        all_env_temp.append(temp)
                        mqtt_msgs.append({'topic':f'{self.mqtt_topic}/pack{pack}/temp_env/temp', 'payload': temp})
                    else:
                        all_pcb_temp.append(temp)
                        mqtt_msgs.append({'topic':f'{self.mqtt_topic}/pack{pack}/temp_pcb/temp', 'payload': temp})
            except Exception as e:
                logging.error(f'Unable to convert data for cell temperature, skipping battery pack {pack}')
                continue

            # Get Other information
            try:
                pack_current_raw = int(converted[offset:offset+4],16)
                if pack_current_raw > 32767:
                    pack_current = float((pack_current_raw - 65536) / 100)
                else:
                    pack_current = float(pack_current_raw / 100)
                if pack_current > 0 or pack_current < 0:
                    pack_active = 1
            except Exception as e:
                logging.error(f'Unable to convert data for cell current, skipping battery pack {pack}')
                continue

            mqtt_msgs.append({'topic':f'{self.mqtt_topic}/pack{pack}/current', 'payload': pack_current})
            all_pack_current.append(pack_current)
            offset += 4

            try:
                pack_voltage = float(int(converted[offset:offset+4],16) / 100)
            except Exception as e:
                logging.error(f'Unable to convert data for cell voltage, skipping battery pack {pack}')
                continue
            mqtt_msgs.append({'topic':f'{self.mqtt_topic}/pack{pack}/pack_voltage', 'payload': pack_voltage})
            all_pack_voltage.append(pack_voltage)
            offset += 4

            try:
                pack_ah_available = float(int(converted[offset:offset+4],16) / 100)
            except Exception as e:
                logging.error(f'Unable to convert data for pack AH available, skipping battery pack {pack}')
                continue
            mqtt_msgs.append({'topic':f'{self.mqtt_topic}/pack{pack}/ah_available', 'payload': pack_ah_available})
            all_pack_ah_available.append(pack_ah_available)
            offset += 6

            try:
                pack_ah_total = float(int(converted[offset:offset+4],16) / 100)
            except Exception as e:
                logging.error(f'Unable to convert data for pack AH totals, skipping battery pack {pack}')
                continue
            mqtt_msgs.append({'topic':f'{self.mqtt_topic}/pack{pack}/ah_total', 'payload': pack_ah_total})
            all_pack_ah_total.append(pack_ah_total)
            offset += 4

            try:
                pack_soc = float(int(converted[offset:offset+4],16) / 10)
            except Exception as e:
                logging.error(f'Unable to convert data for SOC, skipping battery pack {pack}')
                continue
            mqtt_msgs.append({'topic':f'{self.mqtt_topic}/pack{pack}/soc', 'payload': pack_soc})
            all_pack_soc.append(pack_soc)
            offset += 4

            try:
                pack_ah_rated = float(int(converted[offset:offset+4],16) / 100)
            except Exception as e:
                logging.error(f'Unable to convert data for pack AH rated, skipping battery pack {pack}')
                continue
            mqtt_msgs.append({'topic':f'{self.mqtt_topic}/pack{pack}/ah_rated', 'payload': pack_ah_rated})
            all_pack_ah_rated.append(pack_ah_rated)
            offset += 4

            try:
                pack_cycles = int(converted[offset:offset+4],16)
            except Exception as e:
                logging.error(f'Unable to convert data for pack cycles, skipping battery pack {pack}')
                continue
            mqtt_msgs.append({'topic':f'{self.mqtt_topic}/pack{pack}/cycles', 'payload': pack_cycles})
            all_pack_cycles.append(pack_cycles)
            offset += 4

            try:
                pack_soh = float(int(converted[offset:offset+4],16) / 10)
            except Exception as e:
                logging.error(f'Unable to convert data for pack SOH, skipping battery pack {pack}')
                continue
            mqtt_msgs.append({'topic':f'{self.mqtt_topic}/pack{pack}/soh', 'payload': pack_soh})
            all_pack_soh.append(pack_soh)
            offset += 4

            try:
                pack_bus_voltage = float(int(converted[offset:offset+4],16) / 100)
            except Exception as e:
                logging.error(f'Unable to convert data for pack bus voltage, skipping battery pack {pack}')
                continue
            mqtt_msgs.append({'topic':f'{self.mqtt_topic}/pack{pack}/bus_voltage', 'payload': pack_bus_voltage})
            all_pack_bus_voltage.append(pack_bus_voltage)
            offset += 4

        # Workaround to catch lowest and highest cell voltage and make it available via our "virtual single battery"
        low_cell = dict(sorted(all_cell_voltages.items(), key = itemgetter(1), reverse = False)[:1])
        (lkey, lval) = low_cell.popitem()
        high_cell = dict(sorted(all_cell_voltages.items(), key = itemgetter(1), reverse = True)[:1])
        (hkey, hval) = high_cell.popitem()
        # low_cell_id = lkey
        self.cells[0].voltage = lval
        # high_cell_id = hkey
        self.cells[15].voltage = hval
        # use workaround to set first cell of "virtual pack" to lowest, last cell to highest cell temperature
        self.cells[0].temp = min(all_cell_temp)
        self.cells[15].temp = max(all_cell_temp)
        all_cell_temp.sort()

        self.voltage = sum(all_pack_voltage) / battery_packs_online
        self.current = sum(all_pack_current)
        mqtt_msgs.append({'topic':f'{self.mqtt_topic}/current_total', 'payload': self.current})
        mqtt_msgs.append({'topic':f'{self.mqtt_topic}/pack_power', 'payload': self.current*self.voltage})
        self.capacity = sum(all_pack_ah_total)
        mqtt_msgs.append({'topic':f'{self.mqtt_topic}/ah_total', 'payload': self.capacity})
        self.cycles = sum(all_pack_cycles) / battery_packs_online
        mqtt_msgs.append({'topic':f'{self.mqtt_topic}/cycles_total', 'payload': self.cycles})
        self.soc = sum(all_pack_soc) / battery_packs_online
        mqtt_msgs.append({'topic':f'{self.mqtt_topic}/soc_total', 'payload': self.soc})
        # lowest temperature
        self.temp1 = min(all_env_temp+all_cell_temp+all_pcb_temp)
        # highest temperature
        self.temp2 = max(all_env_temp+all_cell_temp+all_pcb_temp)
        mqtt_msgs.append({'topic':f'{self.mqtt_topic}/soh_total', 'payload': sum(all_pack_soh) / battery_packs_online})
        if self.mqtt_send:
            publish.multiple(mqtt_msgs, hostname=self.mqtt_host)
        return True

    def test_connection(self):
        result = False
        try:
            result = self.refresh_data()
        except Exception as e:
            logger.error(e)

        return result

    def get_settings(self):
        # After successful  connection get_settings will be call to set up the battery.
        # Set the current limits, populate cell count, etc
        # Return True if success, False for failure
        self.max_battery_charge_current = MAX_BATTERY_CHARGE_CURRENT
        self.max_battery_discharge_current = MAX_BATTERY_DISCHARGE_CURRENT
        self.min_battery_voltage = MIN_CELL_VOLTAGE * 16
        self.max_battery_voltage = MAX_CELL_VOLTAGE * 16
        self.cell_count = 16  # FIXME - required?
        self.hardware_version = "SEPLOS Virtual V1.0"
        logger.info(f'Hardware version: {self.hardware_version}')
        return True

    def refresh_data(self):
        if self.read_status_data():
            return True

    def read_status_data(self):
        responses = {}
        ser = serial.Serial(self.port, self.baud_rate, timeout = 0.1, write_timeout = 0.1)

        if ser.is_open:
            for pack_id in range(1,self.battery_packs+1):
                if pack_id < 10:
                    pack_id = '0' + str(pack_id)
                command = self.convert_address(pack_id)
                ser.write(command.encode())
                sleep(0.25)
                len_return_data = ser.inWaiting()
                if len_return_data == 0:
                    logger.error(f'Did not receive valid response from pack {pack_id}')
                    continue
                return_data = ser.read(len_return_data)
                responses[pack_id] = return_data
        ser.close()
        if len(responses) > 0:
            result = self.get_bms_information(responses)
            return result
        else:
            return False