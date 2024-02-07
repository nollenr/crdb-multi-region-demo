#!/usr/bin/env python3
from datetime import datetime as dt
import os
from random import randint
import signal
import sys
# from time import perf_counter
# import time
from uuid import uuid4

from prometheus_client import start_http_server, Summary, Histogram
import random
import time
from time import perf_counter

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine as SAEngine
# from sqlalchemy.orm import scoped_session, sessionmaker

# from web.config import Config
# from movr.movr import MovR
# from movr.models import User, Vehicle  # , Ride
from movr.transactions2 import (
    get_node_info, get_user, get_vehicle, get_users, get_vehicles,
    start_ride, end_ride, read_ride_info,
    add_vehicle_location_history, read_vehicle_last_location, update_vehicle_status,
    read_ride_info_aost
)
from movr.helpers import DemoStats, DemoTimer, run_transaction

STATS_INTERVAL_SECS = 5
NUM_UPDATES_PER_RIDE = 5

DB_URI = os.getenv('DB_URI', 'cockroachdb://root@127.0.0.1:26257/movr_demo?application_name=movr_demo')
READ_USER_HISTOGRAM = Histogram('read_user_latency','DB Time to read a user')
READ_VEHICLE_HISTOGRAM = Histogram('read_vehicle_latency','DB Time to read a vehicle')
UPDATE_VECHICLE_STATUS_HISTOGRAM = Histogram('update_vehicle_status_latency','DB Time to update a vehicle status')
ADD_A_RIDE_HISTOGRAM = Histogram('add_a_ride_latency','DB Time to add a ride')
UPDATE_VEHICLE_LOCATION_HISTOGRAM = Histogram('update_vehicle_location_latency','DB Time to update a vehicle location')
READ_VEHICLE_LOCATION_HISTOGRAM = Histogram('read_vehicle_location_latency','DB Time to read a vehicle location')
END_A_RIDE_HISTOGRAM = Histogram('end_a_ride_latency','DB Time to end a ride')
READ_A_RIDE_SUMMARY_HISTOGRAM = Histogram('read_a_ride_summary_latency','DB Time to read a ride summary')
READ_A_RIDE_SUMMARY_AOST_HISTOGRAM = Histogram('read_a_ride_summary_aost_latency','DB Time to read a ride summary using AOST.')


def demo_flow_once(db_engine: SAEngine, user_ids: list, vehicle_ids: list, op_timer: DemoTimer, stats: DemoStats):
    # Pick a random user
    user_id = user_ids[randint(0, len(user_ids) - 1)]

    # Pick a vehicle
    vehicle_id = vehicle_ids[randint(0, len(vehicle_ids) - 1)]

    # Read the user
    op_timer.start()
    start_txn_time = perf_counter()
    run_transaction(
        db_engine,
        lambda conn: get_user(conn, user_id)
    )
    READ_USER_HISTOGRAM.observe((perf_counter() - start_txn_time)*1000)
    stats.add_to_stats(DemoStats.OP_READ_USER, op_timer.stop())

    # Read the vehicle
    op_timer.start()
    start_txn_time = perf_counter()
    run_transaction(
        db_engine,
        lambda conn: get_vehicle(conn, vehicle_id)
    )
    READ_VEHICLE_HISTOGRAM.observe((perf_counter() - start_txn_time)*1000)
    stats.add_to_stats(DemoStats.OP_READ_VEHICLE, op_timer.stop())

    # Update the vehicle as 'in_use'
    op_timer.start()
    start_txn_time = perf_counter()
    run_transaction(
        db_engine,
        lambda conn: update_vehicle_status(conn, vehicle_id, 'in_use')
    )
    UPDATE_VECHICLE_STATUS_HISTOGRAM.observe((perf_counter() - start_txn_time)*1000)
    stats.add_to_stats(DemoStats.OP_UPDATE_VEHICLE_STATUS, op_timer.stop())

    # Add a ride
    ride_id = uuid4()
    start_time = dt.now()
    op_timer.start()
    start_txn_time = perf_counter()
    run_transaction(
        db_engine,
        lambda conn: start_ride(
            conn, ride_id, user_id, start_time, vehicle_id, 'San Francisco'
        )
    )
    ADD_A_RIDE_HISTOGRAM.observe((perf_counter() - start_txn_time)*1000)
    stats.add_to_stats(DemoStats.OP_INSERT_RIDE, op_timer.stop())

    # Update vechicle location history
    for i in range(NUM_UPDATES_PER_RIDE):
        seen_time = dt.now()
        lat = randint(-180, 180)
        long = randint(-180, 180)

        # Record the current location
        op_timer.start()
        start_txn_time = perf_counter()
        loc_id = run_transaction(
            db_engine,
            lambda conn: add_vehicle_location_history(conn, ride_id, seen_time, lat, long)
        )
        UPDATE_VEHICLE_LOCATION_HISTOGRAM.observe((perf_counter() - start_txn_time)*1000)
        stats.add_to_stats(DemoStats.OP_INSERT_LOCATION, op_timer.stop())

        # Read the latest location
        op_timer.start()
        start_txn_time = perf_counter()
        run_transaction(
            db_engine,
            lambda conn: read_vehicle_last_location(conn, loc_id)
        )
        READ_VEHICLE_LOCATION_HISTOGRAM.observe((perf_counter() - start_txn_time)*1000)
        stats.add_to_stats(DemoStats.OP_READ_LAST_LOCATION, op_timer.stop())

    # End a ride
    end_time = dt.now()
    op_timer.start()
    start_txn_time = perf_counter()
    run_transaction(
        db_engine,
        lambda conn: end_ride(conn, ride_id, end_time)
    )
    END_A_RIDE_HISTOGRAM.observe((perf_counter() - start_txn_time)*1000)
    stats.add_to_stats(DemoStats.OP_UPDATE_RIDE, op_timer.stop())

    # Update the vehicle as 'available'
    op_timer.start()
    start_txn_time = perf_counter()
    run_transaction(
        db_engine,
        lambda conn: update_vehicle_status(conn, vehicle_id, 'available')
    )
    UPDATE_VECHICLE_STATUS_HISTOGRAM.observe((perf_counter() - start_txn_time)*1000)
    stats.add_to_stats(DemoStats.OP_UPDATE_VEHICLE_STATUS, op_timer.stop())

    # Read ride summary
    op_timer.start()
    start_txn_time = perf_counter()
    run_transaction(
        db_engine,
        lambda conn: read_ride_info(conn, ride_id)
    )
    READ_A_RIDE_SUMMARY_HISTOGRAM.observe((perf_counter() - start_txn_time)*1000)
    stats.add_to_stats(DemoStats.OP_READ_RIDE, op_timer.stop())

    # Read ride summary as a follower read
    op_timer.start()
    start_txn_time = perf_counter()
    run_transaction(
        db_engine,
        lambda conn: read_ride_info_aost(conn, ride_id)
    )
    READ_A_RIDE_SUMMARY_AOST_HISTOGRAM.observe((perf_counter() - start_txn_time)*1000)
    stats.add_to_stats(DemoStats.OP_READ_RIDE_AOST, op_timer.stop())

    # Get the node info all of this was run on
    node_info = run_transaction(
        db_engine,
        lambda conn: get_node_info(conn)
    )
    stats.update_node_info(node_info[0], node_info[1])
    


def main():
    start_http_server(8000)
    HOST        = os.getenv('DB_HOST', 'cockroachdb://root@127.0.0.1:26257/movr_demo?application_name=movr_demo')
    USER        = os.getenv('DB_USER', 'bob')
    SSLCERT     = os.getenv('DB_SSLCERT', '/home/ec2-user/certs/client.bob.crt')
    SSLKEY      = os.getenv('DB_SSLKEY', '/home/ec2-user/certs/client.bob.key')
    SSLROOTCERT = os.getenv('DB_SSLROOTCERT', '/home/ec2-user/certs/ca.crt')
    SSLMODE     = os.getenv('DB_SSLMODE', 'require')

    args = {
        "host":        HOST,
        "port":        "26257",
        "user":        USER,
        "dbname":      "movr_demo",
        "sslcert":     SSLCERT,
        "sslkey":      SSLKEY,
        "sslrootcert": SSLROOTCERT,
        "sslmode":     SSLMODE,
        "application_name" : "movr_demo",
    }
    db_engine = create_engine("cockroachdb://", connect_args=args)

    # Get node info to display at startup
    node_info = run_transaction(
        db_engine,
        lambda conn: get_node_info(conn)
    )

    print(f"Connected to node id {node_info[0]} at {node_info[1]}")

    stats = DemoStats(STATS_INTERVAL_SECS, node_info[0], node_info[1])
    op_timer = DemoTimer()

    # Build a list of users and vehicles so we can randomly pull from them
    # user_ids = []
    # with movr.sessionmaker() as session:
    #     user_ids = [row.id for row in session.query(User.id).all()]
    user_ids = run_transaction(
        db_engine,
        lambda conn: get_users(conn)
    )

    print(f"{len(user_ids)} users found")

    # vehicle_ids = []
    # with movr.sessionmaker() as session:
    #     vehicle_ids = [row.id for row in session.query(Vehicle.id).all()]
    vehicle_ids = run_transaction(
        db_engine,
        lambda conn: get_vehicles(conn)
    )
    print(f"{len(vehicle_ids)} vehicles found")

    while True:

        demo_flow_once(db_engine, user_ids, vehicle_ids, op_timer, stats)

        stats.display_if_ready()
        # print()
        # time.sleep(1)


if __name__ == '__main__':

    # Gracefully handle CTRL-C
    def sigint_handler(signal, frame):
        sys.exit(0)
    signal.signal(signal.SIGINT, sigint_handler)

    main()
