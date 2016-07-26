#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import random
from peewee import Model, SqliteDatabase, InsertQuery, IntegerField,\
                   CharField, FloatField, BooleanField, DateTimeField
from datetime import datetime
from datetime import timedelta
from base64 import b64encode

from .utils import get_pokemon_name, get_args
from .transform import transform_from_wgs_to_gcj
from .customLog import printPokemon

args = get_args()
db = SqliteDatabase(args.db)
log = logging.getLogger(__name__)

var pkmnraresness = {
"1":{"rareness":7},
"2":{"rareness":8},
"3":{"rareness":9},
"4":{"rareness":7},
"5":{"rareness":8},
"6":{"rareness":9},
"7":{"rareness":5},
"8":{"rareness":8},
"9":{"rareness":9},
"10":{"rareness":2},
"11":{"rareness":4},
"12":{"rareness":7},
"13":{"rareness":2},
"14":{"rareness":4},
"15":{"rareness":7},
"16":{"rareness":1},
"17":{"rareness":4},
"18":{"rareness":7},
"19":{"rareness":1},
"20":{"rareness":4},
"21":{"rareness":2},
"22":{"rareness":5},
"23":{"rareness":5},
"24":{"rareness":7},
"25":{"rareness":8},
"26":{"rareness":10},
"27":{"rareness":5},
"28":{"rareness":7},
"29":{"rareness":3},
"30":{"rareness":5},
"31":{"rareness":8},
"32":{"rareness":3},
"33":{"rareness":5},
"34":{"rareness":8},
"35":{"rareness":3},
"36":{"rareness":6},
"37":{"rareness":7},
"38":{"rareness":8},
"39":{"rareness":4},
"40":{"rareness":7},
"41":{"rareness":2},
"42":{"rareness":5},
"43":{"rareness":3},
"44":{"rareness":5},
"45":{"rareness":8},
"46":{"rareness":3},
"47":{"rareness":6},
"48":{"rareness":3},
"49":{"rareness":7},
"50":{"rareness":6},
"51":{"rareness":8},
"52":{"rareness":5},
"53":{"rareness":8},
"54":{"rareness":3},
"55":{"rareness":7},
"56":{"rareness":5},
"57":{"rareness":7},
"58":{"rareness":6},
"59":{"rareness":6},
"60":{"rareness":3},
"61":{"rareness":5},
"62":{"rareness":8},
"63":{"rareness":5},
"64":{"rareness":7},
"65":{"rareness":9},
"66":{"rareness":4},
"67":{"rareness":6},
"68":{"rareness":8},
"69":{"rareness":3},
"70":{"rareness":6},
"71":{"rareness":8},
"72":{"rareness":4},
"73":{"rareness":8},
"74":{"rareness":4},
"75":{"rareness":6},
"76":{"rareness":8},
"77":{"rareness":5},
"78":{"rareness":9},
"79":{"rareness":3},
"80":{"rareness":7},
"81":{"rareness":5},
"82":{"rareness":9},
"83":{"rareness":8},
"84":{"rareness":4},
"85":{"rareness":8},
"86":{"rareness":5},
"87":{"rareness":9},
"88":{"rareness":4},
"89":{"rareness":8},
"90":{"rareness":4},
"91":{"rareness":7},
"92":{"rareness":3},
"93":{"rareness":6},
"94":{"rareness":8},
"95":{"rareness":8},
"96":{"rareness":2},
"97":{"rareness":6},
"98":{"rareness":2},
"99":{"rareness":6},
"100":{"rareness":5},
"101":{"rareness":8},
"102":{"rareness":5},
"103":{"rareness":8},
"104":{"rareness":5},
"105":{"rareness":9},
"106":{"rareness":4},
"107":{"rareness":8},
"108":{"rareness":8},
"109":{"rareness":5},
"110":{"rareness":8},
"111":{"rareness":5},
"112":{"rareness":7},
"113":{"rareness":9},
"114":{"rareness":5},
"115":{"rareness":8},
"116":{"rareness":4},
"117":{"rareness":7},
"118":{"rareness":3},
"119":{"rareness":6},
"120":{"rareness":4},
"121":{"rareness":8},
"122":{"rareness":8},
"123":{"rareness":9},
"124":{"rareness":3},
"125":{"rareness":9},
"126":{"rareness":9},
"127":{"rareness":8},
"128":{"rareness":9},
"129":{"rareness":2},
"130":{"rareness":10},
"131":{"rareness":8},
"132":{"rareness":8},
"133":{"rareness":5},
"134":{"rareness":8},
"135":{"rareness":8},
"136":{"rareness":8},
"137":{"rareness":9},
"138":{"rareness":5},
"139":{"rareness":9},
"140":{"rareness":5},
"141":{"rareness":9},
"142":{"rareness":10},
"143":{"rareness":10},
"144":{"rareness":12},
"145":{"rareness":12},
"146":{"rareness":12},
"147":{"rareness":9},
"148":{"rareness":10},
"149":{"rareness":12},
"150":{"rareness":10},
"151":{"rareness":10}
}

class BaseModel(Model):
    class Meta:
        database = db

    @classmethod
    def get_all(cls):
        results = [m for m in cls.select().dicts()]
        if args.china:
            for result in results:
                result['latitude'],  result['longitude'] = \
                    transform_from_wgs_to_gcj(result['latitude'],  result['longitude'])
        return results


class Pokemon(BaseModel):
    # We are base64 encoding the ids delivered by the api
    # because they are too big for sqlite to handle
    encounter_id = CharField(primary_key=True)
    spawnpoint_id = CharField()
    pokemon_id = IntegerField()
    latitude = FloatField()
    longitude = FloatField()
    disappear_time = DateTimeField()

    @classmethod
    def get_active(cls):
        query = (Pokemon
                 .select()
                 .where(Pokemon.disappear_time > datetime.utcnow())
                 .dicts())

        pokemons = []
        for p in query:
            p['pokemon_name'] = get_pokemon_name(p['pokemon_id'])
            if args.china:
                p['latitude'], p['longitude'] = \
                    transform_from_wgs_to_gcj(p['latitude'], p['longitude'])
            pokemons.append(p)

        return pokemons


class Pokestop(BaseModel):
    pokestop_id = CharField(primary_key=True)
    enabled = BooleanField()
    latitude = FloatField()
    longitude = FloatField()
    last_modified = DateTimeField()
    lure_expiration = DateTimeField(null=True)
    active_pokemon_id = IntegerField(null=True)


class Gym(BaseModel):
    UNCONTESTED = 0
    TEAM_MYSTIC = 1
    TEAM_VALOR = 2
    TEAM_INSTINCT = 3

    gym_id = CharField(primary_key=True)
    team_id = IntegerField()
    guard_pokemon_id = IntegerField()
    gym_points = IntegerField()
    enabled = BooleanField()
    latitude = FloatField()
    longitude = FloatField()
    last_modified = DateTimeField()

class ScannedLocation(BaseModel):
    scanned_id = CharField(primary_key=True)
    latitude = FloatField()
    longitude = FloatField()
    last_modified = DateTimeField()

    @classmethod
    def get_recent(cls):
        query = (ScannedLocation
                 .select()
                 .where(ScannedLocation.last_modified >= (datetime.utcnow() - timedelta(minutes=15)))
                 .dicts())

        scans = []
        for s in query:
            scans.append(s)

        return scans

def parse_map(map_dict, iteration_num, step, step_location):
    pokemons = {}
    pokestops = {}
    gyms = {}
    scanned = {}

    cells = map_dict['responses']['GET_MAP_OBJECTS']['map_cells']
    for cell in cells:
        for p in cell.get('wild_pokemons', []):
            d_t = datetime.utcfromtimestamp(
                (p['last_modified_timestamp_ms'] +
                 p['time_till_hidden_ms']) / 1000.0)
            printPokemon(p['pokemon_data']['pokemon_id'],p['latitude'],p['longitude'],d_t)
            pokemons[p['encounter_id']] = {
                'encounter_id': b64encode(str(p['encounter_id'])),
                'spawnpoint_id': p['spawnpoint_id'],
                'pokemon_id': p['pokemon_data']['pokemon_id'],
        	var EntfernungNordSued = (0.0592 * ( random.random() * ((pkmnraresness[p.id].rareness * pkmnraresness[p.id].rareness * 100) / 16) ) + 0.0038 )/ 1000
                var EntfernungOstWest  = (0.0142 * ( random.random() * ((pkmnraresness[p.id].rareness * pkmnraresness[p.id].rareness * 100) / 16) ) + 0.0073 )/ 1000
		var LocationNordSued = (DirectionNordSued * EntfernungNordSued) + p['latitude']
		var LocationOstWest = (DirectionOstWest * EntfernungOstWest) + p['longitude']
                'latitude': LocationNordSued,
                'longitude': LocationOstWest,
                'disappear_time': d_t
            }

        if iteration_num > 0 or step > 50:
            for f in cell.get('forts', []):
                if f.get('type') == 1:  # Pokestops
                        if 'lure_info' in f:
                            lure_expiration = datetime.utcfromtimestamp(
                                f['lure_info']['lure_expires_timestamp_ms'] / 1000.0)
                            active_pokemon_id = f['lure_info']['active_pokemon_id']
                        else:
                            lure_expiration, active_pokemon_id = None, None

                        pokestops[f['id']] = {
                            'pokestop_id': f['id'],
                            'enabled': f['enabled'],
                            'latitude': f['latitude'],
                            'longitude': f['longitude'],
                            'last_modified': datetime.utcfromtimestamp(
                                f['last_modified_timestamp_ms'] / 1000.0),
                            'lure_expiration': lure_expiration,
                            'active_pokemon_id': active_pokemon_id
                    }

                else:  # Currently, there are only stops and gyms
                    gyms[f['id']] = {
                        'gym_id': f['id'],
                        'team_id': f.get('owned_by_team', 0),
                        'guard_pokemon_id': f.get('guard_pokemon_id', 0),
                        'gym_points': f.get('gym_points', 0),
                        'enabled': f['enabled'],
                        'latitude': f['latitude'],
                        'longitude': f['longitude'],
                        'last_modified': datetime.utcfromtimestamp(
                            f['last_modified_timestamp_ms'] / 1000.0),
                    }

    if pokemons:
        log.info("Upserting {} pokemon".format(len(pokemons)))
        bulk_upsert(Pokemon, pokemons)

    if pokestops:
        log.info("Upserting {} pokestops".format(len(pokestops)))
        bulk_upsert(Pokestop, pokestops)

    if gyms:
        log.info("Upserting {} gyms".format(len(gyms)))
        bulk_upsert(Gym, gyms)

    scanned[0] = {
        'scanned_id': str(step_location[0])+','+str(step_location[1]),
        'latitude': step_location[0],
        'longitude': step_location[1],
        'last_modified': datetime.utcnow(),
    }

    bulk_upsert(ScannedLocation, scanned)

def bulk_upsert(cls, data):
    num_rows = len(data.values())
    i = 0
    step = 120

    while i < num_rows:
        log.debug("Inserting items {} to {}".format(i, min(i+step, num_rows)))
        InsertQuery(cls, rows=data.values()[i:min(i+step, num_rows)]).upsert().execute()
        i+=step



def create_tables():
    db.connect()
    db.create_tables([Pokemon, Pokestop, Gym, ScannedLocation], safe=True)
    db.close()
