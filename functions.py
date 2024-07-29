import psycopg
from datetime import datetime
from local_settings import DBNAME, DBUSER, DBPASSWORD, DBPORT, DBHOST


# -> test functions section
async def file_write(data: dict, filename: str):

    if "data" not in data:
        return

    with open(filename, "at") as out:
        out.write("-> " + str(datetime.now()))
        out.write("\n")
        imei = data["login"]["imei"]
        for d in data["data"]:
            date = datetime.strptime(d["date"] + d["time"], "%d%m%y%H%M%S")
            lat = d["latitude"]
            lon = d["longitude"]
            speed = d["speed"]
            course = d["course"]
            height = d["height"]
            sats = d["sats"]
            out.write(
                f"imei: {imei} date: {date} lat: {lat} lon: {lon} speed: {speed} course: {course} heght: {height} sats: {sats}\n"
            )
        out.write("\n\n")


# -> write to database function
async def db_write(data: dict):

    if "data" not in data:
        return

    # Connect to an existing database
    with psycopg.connect(
        f"host={DBHOST} port={DBPORT} dbname={DBNAME} user={DBUSER} password={DBPASSWORD}"
    ) as conn:
        with conn.cursor() as cur:

            imei = data["login"]["imei"]
            for d in data["data"]:
                date = datetime.strptime(d["date"] + d["time"], "%d%m%y%H%M%S")
                lat = float(d["latitude"])
                lon = float(d["longitude"])
                speed = int(d["speed"])
                course = int(d["course"])
                height = int(d["height"])
                sats = int(d["sats"])

                # Pass data to fill a query placeholders and let Psycopg perform
                # the correct conversion (no SQL injections!)
                # id | imei | date | lat | lon | speed | course | height | sats

                # cur.execute(
                #     f"""
                #     INSERT INTO transport_transportpoint (imei, date, lat, lon, speed, course, height, sats)
                #     VALUES ({imei}, '{date}', {lat}, {lon}, {speed}, {course}, {height}, {sats}"""
                # )

                cur.execute(
                    """
                    INSERT INTO transport_transportpoint (imei, date, lat, lon, speed, course, height, sats)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (imei, date, lat, lon, speed, course, height, sats),
                )

            # Make the changes to the database persistent
            conn.commit()


def parse_wialon_packet(data: str) -> dict:
    data = data.strip()

    parsed_data = {}

    # -> splitting lines
    for line in data.splitlines():
        chunks = line.split("#")

        p_type = chunks[1]
        p_data = chunks[2]

        # -> identifying packet type

        # --> login packet
        if p_type == "L":
            parsed_data["login"] = parse_login_packet(p_data)

        # --> blackbox packet
        elif p_type == "B":
            parsed_data["data"] = parse_wialon_ips_data(p_data)

        # --> undefined packet
        else:
            pass

    return parsed_data


def parse_login_packet(p_data: str) -> dict:
    fields = p_data.strip().split(";")
    imei = fields[0]
    password = fields[1]
    return {
        "imei": imei,
        "password": password,
    }


# example
#   #D#date;time;lat1;lat2;lon1;lon2;speed;course;height;sats;hdop;inputs;outputs;adc;ibutton;params\r\n
def parse_wialon_ips_data(packet_data: str) -> list[dict]:
    packet_data = packet_data.strip()
    packets = packet_data.split("|")
    parsed_data = []
    for packet in packets:
        fields = packet.split(";")
        parsed_packet = {
            "date": fields[0],
            "time": fields[1],
            "latitude": convert_to_decimal(fields[2], fields[3]),
            "longitude": convert_to_decimal(fields[4], fields[5]),
            "speed": fields[6],
            "course": fields[7],
            "height": fields[8],
            "sats": fields[9],
        }
        parsed_data.append(parsed_packet)
    return parsed_data


def convert_to_decimal(coord: float, direction: str) -> float:
    coord = coord.lstrip("0")
    degrees = int(coord[:2])
    minutes = float(coord[2:])
    decimal = degrees + minutes / 60
    if direction in ["S", "W"]:
        decimal = -decimal
    return decimal
