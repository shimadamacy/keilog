#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""InfluxDBをアップロードする機能をもつクラスを定義する

"""
import threading
import requests
import sys
import queue
from influxdb import InfluxDBClient
from keilib.worker import Worker

from logging import getLogger, StreamHandler, DEBUG
logger = getLogger(__name__)

class InfluxDBUpdater ( Worker ):
    def __init__( self, record_que, host, port, db):
        super().__init__()

        self.db = db
        self.record_que = record_que
        self.client = InfluxDBClient(host, port)
        
        while True:
            try:
                self.client.create_database(self.db)
                break
            except requests.exceptions.ConnectionError:
                continue

    def run ( self ):
        logger.info('[START]')
        while not self.stopEvent.is_set():
            try:
                unit, sensor, value, dataid = self.record_que.get(timeout=3)
                logger.debug(f"{sensor} {dataid} {value}")
                if sensor == 'E7':
                    #logger.debug(f"{filename} {date} {time} {round(value, 4)}")
                   
                    body = [{
                            'measurement': 'power',
                            'fields': {
                                'power': round(value, 4),
                            }
                        }]

                elif sensor == 'E8':
                    #logger.debug(f"{filename} {date} {time} {value}")
                    body = [{
                            'measurement': 'currents',
                            'fields': {
                                'currents': value,
                            }
                        }]
                logger.debug(body)
                self.client.write_points(body, database=self.db)
            except:
                # logger.debug('upload que is empty')
                continue
        logger.info('[STOP]')
