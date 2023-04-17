from datetime import datetime as dt
# from time import perf_counter
from typing import List
from uuid import UUID

from sqlalchemy.engine import Connection, Row
from sqlalchemy.sql import text


# ----
# Node Info
# ----
def get_node_info(conn: Connection) -> str:
    sql = text("select node_id, locality from crdb_internal.gossip_nodes where node_id = crdb_internal.node_id()")
    result = conn.execute(sql).one()
    return result

# ----
# User
# ----
def get_user(conn: Connection, user_id: UUID) -> UUID:
    sql = text("SELECT id, city, name, address, credit_card FROM users WHERE id = :id")
    sql = sql.bindparams(id=user_id)
    result = conn.execute(sql).one()
    return result.id


def get_users(conn: Connection) -> List[UUID]:
    sql = text("SELECT id FROM users")
    return [row.id for row in conn.execute(sql).all()]


# -------
# Vehicle
# -------
def get_vehicle(conn: Connection, vehicle_id: UUID) -> UUID:
    sql = text("SELECT id, city, owner_id, creation_time, status, current_location, ext FROM vehicles WHERE id = :id")
    sql = sql.bindparams(id=vehicle_id)
    result = conn.execute(sql).one()
    return result.id


def get_vehicles(conn: Connection) -> List[UUID]:
    sql = text("SELECT id FROM vehicles")
    return [row.id for row in conn.execute(sql).all()]


def update_vehicle_status(conn: Connection, vehicle_id: UUID, status: str) -> UUID:
    sql = text("UPDATE vehicles SET status = :status WHERE id = :id RETURNING id")
    sql = sql.bindparams(id=vehicle_id, status=status)
    result = conn.execute(sql).one()
    return result.id


# ----
# Ride
# ----
def start_ride(conn: Connection, ride_id: UUID, user_id: UUID, start_time: dt, vehicle_id: UUID, city: str) -> UUID:
    sql = text(
        "INSERT INTO rides (id, start_time, city, vehicle_city, rider_id, vehicle_id) "
        "VALUES (:id, :start_time, :city, :vehicle_city, :rider_id, :vehicle_id)"
    )
    sql = sql.bindparams(id=ride_id, start_time=start_time, city=city, vehicle_city=city, rider_id=user_id, vehicle_id=vehicle_id)
    conn.execute(sql)
    return ride_id


def end_ride(conn: Connection, ride_id: UUID, end_time: dt) -> UUID:
    sql = text("UPDATE rides SET end_time = :end_time WHERE id = :id")
    sql = sql.bindparams(id=ride_id, end_time=end_time)
    conn.execute(sql)
    return ride_id


def read_ride_info(conn: Connection, ride_id: UUID) -> Row:
    sql = text(
        'SELECT id, city, rider_id, vehicle_id, start_address, end_address, start_time, end_time, revenue '
        'FROM rides where id = :id'
    )
    sql = sql.bindparams(id=ride_id)
    return conn.execute(sql).one_or_none()


def read_ride_info_aost(conn: Connection, ride_id: UUID) -> Row:
    sql = text(
        'SELECT id, city, rider_id, vehicle_id, start_address, end_address, start_time, end_time, revenue '
        'FROM rides AS OF SYSTEM TIME follower_read_timestamp() WHERE id = :id'
    )
    sql = sql.bindparams(id=ride_id)
    return conn.execute(sql).one_or_none()


# ------------------------
# Vehicle location history
# ------------------------
def add_vehicle_location_history(conn: Connection, ride_id: UUID, seen_time: dt, lat: float, long: float) -> UUID:
    sql = text(
        'INSERT INTO vehicle_location_histories (id, ride_id, "timestamp", lat, long) '
        'VALUES (gen_random_uuid(), :ride_id, :seen_time, :lat, :long) RETURNING id'
    )
    sql = sql.bindparams(ride_id=ride_id, seen_time=seen_time, lat=lat, long=long)
    conn.execute(sql)
    return ride_id


def read_vehicle_last_location(conn: Connection, loc_id: UUID) -> Row:
    sql = text(
        'SELECT ride_id, "timestamp", city, lat, long '
        'FROM vehicle_location_histories '
        'WHERE crdb_region = CAST (gateway_region() AS crdb_internal_region) AND id = :id'
    )
    sql = sql.bindparams(id=loc_id)
    results = conn.execute(sql)
    return results


# # ------------------------
# # Vehicle location history
# # ------------------------
# def add_vehicle_location_history(conn: Connection, ride_id: UUID, seen_time: dt, lat: float, long: float) -> UUID:
#     sql = text(
#         'INSERT INTO vehicle_location_histories (crdb_region, ride_id, "timestamp", lat, long) '
#         'VALUES (CAST(gateway_region() as crdb_internal_region), :ride_id, :seen_time, :lat, :long)'
#     )
#     st = perf_counter()
#     conn.execute(sql, ride_id=ride_id, seen_time=seen_time, lat=lat, long=long)
#     # print(perf_counter() - st)
#     return ride_id


# def read_vehicle_last_location(conn: Connection, ride_id: UUID) -> Row:
#     sql = text(
#         'SELECT ride_id, "timestamp", city, lat, long '
#         'FROM vehicle_location_histories '
#         'WHERE crdb_region = CAST (gateway_region() AS crdb_internal_region) AND ride_id = :ride_id '
#         'ORDER BY "timestamp" DESC '
#         'LIMIT 1'
#     )
#     return conn.execute(sql, ride_id=ride_id)
