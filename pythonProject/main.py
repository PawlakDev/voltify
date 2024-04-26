import signal
from datetime import datetime, timedelta

import mysql.connector
import time

# Timer execution time
INTERVAL = 1
HOURCOUNTER = 0

# So that you can turn off the program when the timer is on
# Otherwise, an error would appear
signal.signal(signal.SIGINT, signal.default_int_handler)


# MySQL connection configuration
def connectToDatabase():
    try:
        conn = mysql.connector.connect(
            host='mysql30.mydevil.net',
            user='m1028_devicedata',
            password='1(rw(aiX_95TMTY',
            database='m1028_devicedata'
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None


# I get all the ids and cast them to int
def getDevicesIds(Conn):
    try:
        cursor = Conn.cursor()
        cursor.execute("SELECT id FROM devices")
        results = cursor.fetchall()
        results_int = [int(row[0]) for row in results]
        return results_int
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None
    finally:
        cursor.close()


# Timer that saves data every "INTERVAL" seconds
def timer(Interval, Ids, Conn):
    try:
        while True:
            for id in Ids:
                saveData(id, Conn)
            time.sleep(Interval)
    except KeyboardInterrupt:
        print("Timer stopped.")


# Here will be the rest api for get
# Handle the exception when null is returned by the rest api
def getData(Login, Password, Ip):
    return 20


# I get the line where the device_id is designated
def getDeviceById(Id, Conn):
    cursor = Conn.cursor()
    cursor.execute("SELECT * FROM devices WHERE id=%s", (Id,))
    return cursor.fetchone()


# Returns the number of rows assigned to a given device id casting to int
def getCountRowsByDeviceId(Id, Conn):
    cursor = Conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM energy_consumption WHERE device_id = %s", (Id,))
    result = cursor.fetchone()[0]
    return int(result)


# Returns all energy values for a given device id from the table with values from minutes
def getAllEnergyConsumptionByDeviceId(Id, Conn):
    cursor = Conn.cursor()
    cursor.execute("SELECT * FROM energy_consumption WHERE device_id = %s", (Id,))
    return cursor.fetchall()


# Energy recording every minute
def insertEnergyConsumption(Device_id, Current_timestamp, Energy):
    print("Inserting Energy Consumption for", Device_id)
    cursor = conn.cursor()
    query = "INSERT INTO energy_consumption(device_id, timestamp, consumption) VALUES (%s, %s, %s)"
    data = (Device_id, Current_timestamp, Energy)
    cursor.execute(query, data)
    try:
        conn.commit()
        print("Saved Energy Consumption for", Device_id)
    except Exception as e:
        conn.rollback()  # Rollback the transaction if an error occurs
        print(f"Failed to save Energy Consumption for {Device_id}. Error: {e}")


# Record every hour + recording logic
def insertHourlyEnergyConsumption(DeviceId, Average, Current_timestamp):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM energy_consumption_hourly WHERE device_id = %s", (DeviceId,))
    rows = cursor.fetchall()
    if rows:
        rowHour = []
        rowId = []
        for row in rows:
            (id, device_id, start_timestamp, end_timestamp, average_consumption) = row
            rowHour.append(start_timestamp.strftime("%H"))
            rowId.append(id)

        currentHour = Current_timestamp.strftime("%H")
        if currentHour in rowHour:
            matching_id = [rowId[i] for i, hour in enumerate(rowHour) if hour == currentHour]

            cursor.execute("SELECT * FROM energy_consumption_hourly WHERE device_id = %s", (matching_id[0],))
            rows = cursor.fetchone()
            for row in rows:
                (id, device_id, start_timestamp, end_timestamp, average_consumption) = row

                # Save history
                query = "INSERT INTO energy_consumption_hourly_history(device_id, start_timestamp, end_timestamp, average_consumption) VALUES (%s, %s, %s, %s)"
                data = (device_id, start_timestamp, end_timestamp, average_consumption)
                cursor.execute(query, data)
                try:
                    conn.commit()
                    print("Saved Energy Consumption History for", device_id)
                except Exception as e:
                    conn.rollback()  # Rollback the transaction if an error occurs
                    print(f"Failed to save Energy Consumption for {device_id}. Error: {e}")

                cursor.execute("DELETE FROM energy_consumption_hourly WHERE id = %s", (device_id,))
                try:
                    conn.commit()
                    print("Deleted Energy Consumption for", device_id)
                except Exception as e:
                    conn.rollback()  # Rollback the transaction if an error occurs
                    print(f"Failed to Delete Energy Consumption for {device_id}. Error: {e}")

    # add 60 minutes to timestamp
    current_timestamp_plus_60_minutes = Current_timestamp + timedelta(minutes=60)
    query = "INSERT INTO energy_consumption_hourly(device_id, start_timestamp, end_timestamp, average_consumption) VALUES (%s, %s, %s, %s)"
    data = (DeviceId, Current_timestamp, current_timestamp_plus_60_minutes, Average)
    cursor.execute(query, data)
    try:
        conn.commit()
        print("Saved Energy Consumption for", DeviceId)
    except Exception as e:
        conn.rollback()  # Rollback the transaction if an error occurs
        print(f"Failed to save Energy Consumption for {DeviceId}. Error: {e}")


def insertDailyEnergyConsumption(DeviceId, Average, Current_timestamp):
    return 0


def deleteAllRowsByIdTableMinutes(deviceId, conn):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM energy_consumption WHERE device_id = %s", (deviceId,))
    conn.commit()


def getAllEnergyConsumptionHourlyByDeviceId(Id, Conn):
        cursor = Conn.cursor()
        cursor.execute("SELECT * FROM energy_consumption_hourly WHERE device_id = %s", (Id,))
        return cursor.fetchall()


def getHourlyCountRowsByDeviceId(Id, Conn):
    cursor = Conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM energy_consumption_hourly WHERE device_id = %s", (Id,))
    result = cursor.fetchone()[0]
    return int(result)


# Main save function (there are execute other functions)
def saveData(id, conn):
    global HOURCOUNTER
    deviceInfo = getDeviceById(id, conn)
    if deviceInfo:
        id, login, passowrd, ip, = deviceInfo
        energy = getData(login, passowrd, ip)
        current_timestamp = datetime.now()
        countRows = getCountRowsByDeviceId(id, conn)
        if countRows and countRows > 59:
            energyConsumptions = getAllEnergyConsumptionByDeviceId(id, conn)

            if energyConsumptions:
                consumptionAvg = 0
                deviceid = 0
                for energyConsumption in energyConsumptions:
                    id, device_id, timestamp, consumption = energyConsumption
                    if (consumption):
                        consumptionAvg += consumption
                        deviceid = device_id
                average = consumptionAvg / countRows
                insertHourlyEnergyConsumption(deviceid, average, current_timestamp)
                deleteAllRowsByIdTableMinutes(deviceid, conn)

                HOURCOUNTER += 1
        else:
            insertEnergyConsumption(id, current_timestamp, energy)

        if HOURCOUNTER > 23:
            energyConsumptionsHourly = getAllEnergyConsumptionHourlyByDeviceId(id, conn)
            countRows = getHourlyCountRowsByDeviceId(id, conn)
            if energyConsumptionsHourly:
                consumptionAvg = 0
                deviceid = 0
                for energyConsumption in energyConsumptionsHourly:
                    id, device_id, timestamp, consumption = energyConsumption
                    if consumption:
                        consumptionAvg += consumption
                        deviceid = device_id
                average = consumptionAvg / countRows
                insertDailyEnergyConsumption(deviceid, average, current_timestamp)
                HOURCOUNTER = 0

        print("Data inserted successfully!")


conn = connectToDatabase()
ids = []

if conn:
    ids = getDevicesIds(conn)
    timer(INTERVAL, ids, conn)
    if ids:
        conn.close()
else:
    print("Failed to connect to the database.")
